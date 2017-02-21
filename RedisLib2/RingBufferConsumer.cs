using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;

namespace RedisLib2
{
    public class MessageLostException : Exception
    {
    }

    public class RingBufferConsumer
    {
        private const long MaxReadSize = 100;
        public const long NotStarted = -2;
        private IDatabase _db;
        private LoadedLuaScript _script;
        public event Action<RedisValue> OnMessageReceived;
        private BlockingCollection<string> _bc = new BlockingCollection<string>();
        private bool _running;

        public long LocalBufferSize
        {
            get { return _bc.Count; }
        }

        public string Topic
        {
            get; private set;
        }

        public string SubscriptionId
        {
            get; private set;
        }

        public long Size
        {
            get; private set;
        }

        public RingBufferConsumer(IDatabase db, IServer server, string topic, int size, string subscriptionId)
        {
            Topic = topic;
            Size = size;
            SubscriptionId = subscriptionId;

            _db = db;
            var _key = $"{RingBufferProducer.KeyPrefix}:{topic}";
            var _consumerIdKey = $"{RingBufferProducer.KeyPrefix}:{topic}:{subscriptionId}";
            var _headKey = $"{RingBufferProducer.KeyPrefix}:{topic}:__head";
            var idKey = $"{RingBufferProducer.KeyPrefix}:{topic}:__mid";

            _script = LuaScript.Prepare(File.ReadAllText("consumer.lua")).Load(server);
            db.StringSet(_consumerIdKey, NotStarted);
        }

        public void Start()
        {
            BeginConsuming();
        }

        private void BeginProcessing()
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
                        o.OnNext(data);
                    }
                }
            });
        }

        private IObserver<RedisValue> o;
        public IObservable<RedisValue> AsObservable()
        {
            return Observable.Create<RedisValue>((IObserver<RedisValue> observer) =>
            {
                o = observer;
                BeginProcessing();
                BeginConsuming();
                return Disposable.Create(() => { _running = false; });
            });
        }

        private RedisValue[] last;

        public void BeginConsuming()
        {
            Task.Run(async () =>
            {
                while (_running)
                {
                    try
                    {
                        var result = await _db.ScriptEvaluateAsync(_script, new { Size = Size, Topic = Topic, SubscriptionId = SubscriptionId, MaxReadSize = MaxReadSize }).ConfigureAwait(false);
                        if (!result.IsNull)
                        {
                            if (result.ToString().StartsWith("AT HEAD"))
                            {
                                Thread.Sleep(20);
                                continue;
                            }

                            if (result.ToString() == "PRODUCER NOT STARTED")
                            {
                                Thread.Sleep(20);
                                continue;
                            }

                            if (result.ToString() == "CONSUMER STARTED")
                            {
                                Thread.Sleep(20);
                                continue;
                            }

                            var range = (RedisValue[])result;

                            foreach (var redisValue in range)
                            {
                                o.OnNext(redisValue);
                            }
                        }
                        else
                        {
                            Thread.Sleep(20);
                        }
                    }
                    catch (Exception e)
                    {
                        _running = false;
                        o.OnError(e);
                    }
                }
                _bc.CompleteAdding();
            });
        }
    }
}
