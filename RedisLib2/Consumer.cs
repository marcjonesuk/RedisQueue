using StackExchange.Redis;
using System;
using System.IO;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace RedisLib2
{
    public class MessageLostException : Exception
    {
    }

    public class ConsumerLappedException : Exception
    {
    }

    public class ScriptManager
    {
    }

    public enum From
    {
        Message,
        Offset,
        LastAcked,
        Head
    }

    public class Consumer
    {
        public RingBuffer Queue { get; private set; }
        public long Head { get; private set; }
        public long Cursor { get; private set; }

        private LoadedLuaScript _script;
        private bool _running;
        private ConsumerOptions _options;
        private string _subscriptionPositionKey;

        internal Consumer(RingBuffer queue, ConsumerOptions options = null)
        {
            Queue = queue;

            if (options == null)
                options = new ConsumerOptions();

            _options = options;
            _subscriptionPositionKey = Queue.DbKey.GetSubscriptionKey(_options.SubscriptionId);

            _script = LuaScript.Prepare(ScriptPreprocessor(File.ReadAllText("Scripts/consume.lua"))).Load(Queue.Server);
        }

        private bool _started;
        private IObservable<Message> _observable;
        public IObservable<Message> Start(From? startFrom = null, long? value = null)
        {
            if (startFrom == null)
                startFrom = From.Head;

            return Observable.Create(async (IObserver<Message> observer) =>
            {
                if (!_started)
                {
                    _started = true;
                    if (!_running)
                    {
                        _running = true;
                        switch (startFrom)
                        {
                            case From.Head:
                                await Queue.Database.StringSetAsync(_subscriptionPositionKey, RingBuffer.StartAtHead);
                                break;

                            case From.Offset:
                                if (!value.HasValue)
                                    throw new ArgumentNullException(nameof(value));

                                var head = await Queue.GetHeadPosition();
                                var start = Math.Max(head - value.Value, -1);
                                await Queue.Database.StringSetAsync(_subscriptionPositionKey, start);
                                break;

                            case From.Message:
                                if (!value.HasValue)
                                    throw new ArgumentNullException(nameof(value));

                                throw new NotSupportedException("StartFrom.Message");

                            case From.LastAcked:
                                throw new NotSupportedException("StartFrom.Last");
                        }

                       //if (offset != null)
                       //{
                       //    var head = await Queue.GetHeadPosition();
                       //    var start = Math.Max(head - offset.Value, -1);
                       //    await Queue.Database.StringSetAsync(_subscriptionPositionKey, start);
                       //}

                       //{
                       //    await Queue.Database.StringSetAsync(_subscriptionPositionKey, RingBuffer.StartAtHead);
                       //}

                       //BeginConsuming(observer);
                   }
                }
                return Disposable.Create(() =>
                {
                    _running = false;
                });
            });
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

        private void BeginConsuming(IObserver<Message> observer)
        {
            Task.Run(async () =>
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

                            if (_options.AckMode == AckMode.Batch || _options.AckMode == AckMode.Message)
                            {
                                var startMessageId = Cursor - messageCount + 1;
                                for (var i = 0; i < messageCount; i++)
                                {
                                    observer.OnNext(new Message() { Key = range[i], Value = range[2 * i + 1], MessageId = startMessageId + i });
                                    if (_options.AckMode == AckMode.Message)
                                    {
                                        //todo
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
                        observer.OnError(e);
                    }
                }
            });
        }
    }
}
