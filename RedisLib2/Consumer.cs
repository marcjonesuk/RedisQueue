using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace RedisLib2
{
    public class Consumer
    {
        public RingBuffer Queue { get; private set; }
        public long Head { get; private set; }
        public long Cursor { get; private set; }

        private LoadedLuaScript _script;
        private bool _running;
        private bool _started;
        private ConsumerOptions _options;
        private string _subscriptionPositionKey;

        private List<IObserver<Message>> _observers = new List<IObserver<Message>>();

        internal Consumer(RingBuffer queue, ConsumerOptions options = null)
        {
            Queue = queue;

            if (options == null)
                options = new ConsumerOptions();

            _options = options;
            _subscriptionPositionKey = Queue.DbKey.GetSubscriptionKey(_options.SubscriptionId);

            _script = LuaScript.Prepare(ScriptPreprocessor(File.ReadAllText("Scripts/consume.lua"))).Load(Queue.Server);
        }

        private string ScriptPreprocessor(string script)
        {
            script = script.Replace("@Length", $"{Queue.Length}");
            script = script.Replace("@HeadKey", $"'{Queue.DbKey.HeadString}'");
            script = script.Replace("@DataKey", $"'{Queue.DbKey.DataHash}'");
            script = script.Replace("@KeyKey", $"'{Queue.DbKey.MessageKeyHash}'");
            script = script.Replace("@SubscriptionKey", $"'{_subscriptionPositionKey}'");
            script = script.Replace("@MaxReadSize", $"{_options.MaxReadSize}");
            script = script.Replace("@ServerAck", $"{(_options.AckMode == AckMode.Server ? 1 : 0)}");
            return script;
        }

        public void AddObserver(IObserver<Message> o)
        {
            lock (_observers)
            {
                _observers.Add(o);

                if (!_running)
                {
                    _running = true;
                }

                if (!_started)
                {
                    _started = true;
                    BeginConsuming();
                }
            }
        }

        public void RemoveObserver(IObserver<Message> o)
        {
            lock (_observers)
            {
                _observers.Remove(o);
                if (_observers.Count == 0)
                {
                    _running = false;
                }
            }
        }

        public StartFrom? StartFrom { get; set; }
        public long? StartFromValue { get; set; }

        public async Task UpdateStartLocation()
        {
            switch (StartFrom)
            {
                case RedisLib2.StartFrom.Head:
                    await Queue.Database.StringSetAsync(_subscriptionPositionKey, RingBuffer.StartAtHead);
                    break;

                case RedisLib2.StartFrom.Offset:
                    if (!StartFromValue.HasValue)
                        throw new ArgumentNullException(nameof(Consumer.StartFromValue));

                    var head = await Queue.GetHeadPosition();
                    var start = Math.Max(head - StartFromValue.Value, -1);
                    await Queue.Database.StringSetAsync(_subscriptionPositionKey, start);
                    break;

                case RedisLib2.StartFrom.Message:
                    if (!StartFromValue.HasValue)
                        throw new ArgumentNullException(nameof(Consumer.StartFromValue));

                    throw new NotSupportedException("StartFrom.Message");

                case RedisLib2.StartFrom.LastAcked:
                    throw new NotSupportedException("StartFrom.Last");
            }
        }

        private async Task BeginConsuming()
        {
            await UpdateStartLocation();
            await Task.Run(async () =>
            {
                while (_running)
                {
                    try
                    {
                        var result = await Queue.Database.ScriptEvaluateAsync(_script).ConfigureAwait(false);
                        if (!result.IsNull)
                        {
                            var s = result.ToString();
                            if (s == RingBuffer.RingBufferResponse.AT_HEAD
                                    || s == RingBuffer.RingBufferResponse.PRODUCER_NOT_STARTED
                                    || s == RingBuffer.RingBufferResponse.CONSUMER_SYNCED
                                    || s == RingBuffer.RingBufferResponse.STARTED)
                            {
                                Thread.Sleep(10);
                                continue;
                            }

                            if (s == RingBuffer.RingBufferResponse.LAPPED)
                                throw new ConsumerLappedException();

                            var range = (RedisValue[])result;
                            var messageCount = (range.Length - 2) / 2;
                            Cursor = long.Parse(range[messageCount * 2]);
                            Head = long.Parse(range[messageCount * 2 + 1]);

                            lock (_observers)
                            {
                                if (_options.AckMode == AckMode.Batch || _options.AckMode == AckMode.Message)
                                {
                                    var startMessageId = Cursor - messageCount + 1;
                                    for (var i = 0; i < messageCount; i++)
                                    {
                                        var message = new Message() { Key = range[i * 2], Value = range[2 * i + 1], MessageId = startMessageId + i };
                                        foreach (var o in _observers)
                                        {
                                            o.OnNext(message);
                                        }

                                        if (_options.AckMode == AckMode.Message)
                                        {
                                            //todo
                                        }
                                    }
                                }
                            }

                            if (_options.AckMode != AckMode.Server)
                                await Queue.Database.StringSetAsync(_subscriptionPositionKey, Cursor).ConfigureAwait(false);
                        }
                        else
                        {
                            throw new ArgumentNullException("Response");
                        }
                    }
                    catch (Exception e)
                    {
                        _running = false;
                        lock (_observers)
                        {
                            foreach (var o in _observers)
                                o.OnError(e);
                        }
                    }
                }
            });
        }
    }
}
