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

    public class RingBufferConsumer
    {
        public RedQ Queue { get; private set; }
        public const long NotStarted = -2;
        private LoadedLuaScript _script;
        private bool _running;
        public string SubscriptionId { get; private set; }
        public long Head { get; private set; }
        public long Cursor { get; private set; }
        private ConsumerOptions _options;

        private string _subscriptionPositionKey;

        internal RingBufferConsumer(RedQ queue, ConsumerOptions options = null)
        {
            Queue = queue;

            if (options == null)
                options = new ConsumerOptions();

            _options = options;
            SubscriptionId = _options.SubscriptionId;
            _subscriptionPositionKey = Queue.DbKey.GetSubscriptionKey(SubscriptionId);

            _script = LuaScript.Prepare(ScriptPreprocessor(File.ReadAllText("RingBuffer/consume.lua"))).Load(Queue.Server);
        }
        
        private IObservable<Message> _observable;
        public IObservable<Message> Start(long? offset = null)
        {
            _running = true;
            if (_observable == null)
            {
                _observable = Observable.Create(async (IObserver<Message> observer) =>
               {
                   if (offset != null)
                   {
                       var head = await Queue.ReadHead();
                       var start = Math.Max(head - offset.Value, 0);
                       await Queue.Database.StringSetAsync(_subscriptionPositionKey, start);
                   }
                   else
                   {
                       await Queue.Database.StringSetAsync(_subscriptionPositionKey, NotStarted);
                   }

                   BeginConsuming(observer);
                   return Disposable.Create(() => { _running = false; });
               });
            }
            return _observable;
        }

        private string ScriptPreprocessor(string script)
        {
            script = script.Replace("@Size", $"{Queue.Length}");

            script = script.Replace("@HeadKey", $"'{Queue.DbKey.HeadString}'");
            script = script.Replace("@DataKey", $"'{Queue.DbKey.DataHash}'");
            script = script.Replace("@KeyKey", $"'{Queue.DbKey.DataHash}'");
            script = script.Replace("@SubscriptionKey", $"'{_subscriptionPositionKey}'");
            script = script.Replace("@MaxReadSize", $"{_options.MaxReadSize}");
            script = script.Replace("@ServerAck", $"{(_options.AckMode == AckMode.Server ? 1 : 0)}");

            if (script.Contains("@"))
                throw new Exception();

            return script;
        }

        public Message[] Query(long count)
        {
            return new Message[5];
        }

        public void BeginConsuming(IObserver<Message> observer)
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
                            if (s == RedQ.RingBufferResponse.AT_HEAD 
                                    || s == RedQ.RingBufferResponse.PRODUCER_NOT_STARTED
                                    || s == RedQ.RingBufferResponse.CONSUMER_SYNCED 
                                    || s == RedQ.RingBufferResponse.STARTED)
                            {
                                Thread.Sleep(10);
                                continue;
                            }

                            if (s == RedQ.RingBufferResponse.LAPPED)
                                throw new ConsumerLappedException();

                            var range = (RedisValue[])result;
                            var messageCount = range.Length - 2;
                            Cursor = long.Parse(range[messageCount]);
                            Head = long.Parse(range[messageCount + 1]);

                            int inc = 2;
                            if (_options.AckMode == AckMode.Batch || _options.AckMode == AckMode.Message)
                            {
                                var startMessageId = Cursor - messageCount;
                                for (var i = 0; i < messageCount; i += inc)
                                {
                                    observer.OnNext(new Message() { Key = range[i], Value = range[i + 1], MessageId = startMessageId + i });
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
