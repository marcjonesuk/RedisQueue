using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
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

    public class RingBufferResponse
    {
        public static readonly string AT_HEAD = "H";
        public static readonly string PRODUCER_NOT_STARTED = "P";
        public static readonly string CONSUMER_SYNCED = "C";
        public static readonly string STARTED = "S";
        public static readonly string LAPPED = "L";
    }

    public enum AckMode
    {
        Server,
        Deliver,
        Process
    }

    public class ConsumerOptions
    {
        public AckMode AckMode { get; private set; }
        public int BufferSize { get; private set; }
        public int MaxReadSize { get; private set; }

        public ConsumerOptions(AckMode ackMode, int bufferSize, int maxReadSize)
        {
            AckMode = ackMode;
            BufferSize = bufferSize;
            MaxReadSize = maxReadSize;
        }
    }
    
    public class RingBufferConsumer
    {
        public const long NotStarted = -2;
        private IDatabase _db;
        private LoadedLuaScript _script;
        private BlockingCollection<string> _bc;
        private bool _running;
        private string _consumerIdKey;

        public string Topic { get; private set; }
        public string SubscriptionId { get; private set; }
        public long Size { get; private set; }
        public long Head { get; private set; }
        public long Position { get; private set; }
        public ConsumerOptions Options { get; private set; }

        public RingBufferConsumer(IDatabase db, IServer server, string topic, int size, string subscriptionId, ConsumerOptions options = null)
        {
            if (options == null)
            {
                options = new ConsumerOptions(AckMode.Deliver, 10000, 5000);
            }
            Options = options;
            Topic = topic;
            Size = size;
            SubscriptionId = subscriptionId;

            _db = db;
            _bc = new BlockingCollection<string>(Options.BufferSize);
            var _key = $"{RingBufferProducer.KeyPrefix}:{topic}";
            _consumerIdKey = $"{RingBufferProducer.KeyPrefix}:{topic}:{subscriptionId}";
            var _headKey = $"{RingBufferProducer.KeyPrefix}:{topic}:__head";

            _script = LuaScript.Prepare(ScriptPreprocessor(File.ReadAllText("RingBuffer/consume.lua"))).Load(server);
        }
        
        private void BeginProcessing(IObserver<RedisValue> observer)
        {
            if (_running)
                throw new InvalidOperationException("Already processing");

            _running = true;
            Task.Run(() =>
            {
                while (!_bc.IsCompleted && _running)
                {
                    string data = null;
                    try
                    {
                        data = _bc.Take();
                    }
                    catch (InvalidOperationException) { }
                    if (data != null)
                    {
                        observer.OnNext(data);
                    }
                }
            });
        }

        private IObservable<RedisValue> _observable;
        public IObservable<RedisValue> AsObservable(long? offset = null)
        {
            if (_observable == null)
            {
                _observable = Observable.Create(async (IObserver<RedisValue> observer) =>
               {
                   if (offset != null)
                   {
                       var head = await _db.StringGetAsync($"__ringbuffer:{Topic}:__head");
                       var start = Math.Max(long.Parse(head) - offset.Value, 0);
                       await _db.StringSetAsync($"__ringbuffer:{Topic}:{SubscriptionId}", start);
                   }
                   else
                   {
                       await _db.StringSetAsync($"__ringbuffer:{Topic}:{SubscriptionId}", NotStarted);
                   }

                   BeginProcessing(observer);
                   BeginConsuming(observer);
                   return Disposable.Create(() => { _running = false; });
               });
            }
            return _observable;
        }

        private string ScriptPreprocessor(string script)
        {
            script = script.Replace("@Size", $"{Size}");
            script = script.Replace("@Topic", $"'{Topic}'");
            script = script.Replace("@SubscriptionId", $"'{SubscriptionId}'");
            script = script.Replace("@MaxReadSize", $"{Options.MaxReadSize}");
            script = script.Replace("@ServerAck", $"{(Options.AckMode == AckMode.Server ? 1 : 0)}");
            return script;
        }

        private long lastId = -1;
        public void BeginConsuming(IObserver<RedisValue> observer)
        {
            Task.Run(async () =>
            {
                while (_running)
                {
                    try
                    {
                        var result = await _db.ScriptEvaluateAsync(_script).ConfigureAwait(false);
                        if (!result.IsNull)
                        {
                            var s = result.ToString();
                            if (s == RingBufferResponse.AT_HEAD 
                                    || s == RingBufferResponse.PRODUCER_NOT_STARTED
                                    || s == RingBufferResponse.CONSUMER_SYNCED 
                                    || s == RingBufferResponse.STARTED)
                            {
                                Thread.Sleep(10);
                                continue;
                            }

                            if (s == RingBufferResponse.LAPPED)
                                throw new ConsumerLappedException();

                            var range = (RedisValue[])result;
                            var messageCount = range.Length - 2;
                            Position = long.Parse(range[messageCount + 1]);
                            Head = long.Parse(range[messageCount + 2]);

                            if (Options.AckMode == AckMode.Process)
                            {
                                for (var i = 0; i < messageCount; i++)
                                    observer.OnNext(range[i]);
                            }
                            else { 
                                for (var i = 0; i < messageCount; i++)
                                    _bc.Add(range[i]);
                            }

                            if (Options.AckMode != AckMode.Server)
                                await _db.StringSetAsync(_consumerIdKey, Position).ConfigureAwait(false);
                        }
                        else
                        {
                            Thread.Sleep(10);
                        }
                    }
                    catch (Exception e)
                    {
                        _running = false;
                        observer.OnError(e);
                    }
                }
                _bc.CompleteAdding();
            });
        }
    }
}
