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

    public class RingBufferCode
    {
        public static readonly string AT_HEAD = "H";
        public static readonly string PRODUCER_NOT_STARTED = "P";
        public static readonly string CONSUMER_SYNCED = "C";
        public static readonly string STARTED = "S";
    }

    public class RingBufferConsumer
    {
        private const long MaxReadSize = 100;
        public const long NotStarted = -2;
        private IDatabase _db;
        private LoadedLuaScript _script;
        private BlockingCollection<string> _bc = new BlockingCollection<string>();
        private bool _running;
        private string _consumerIdKey;

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

        public long Size { get; private set; }
        public long Head { get; private set; }
        public long Position { get; private set; }

        public RingBufferConsumer(IDatabase db, IServer server, string topic, int size, string subscriptionId)
        {
            Topic = topic;
            Size = size;
            SubscriptionId = subscriptionId;

            _db = db;
            var _key = $"{RingBufferProducer.KeyPrefix}:{topic}";
            _consumerIdKey = $"{RingBufferProducer.KeyPrefix}:{topic}:{subscriptionId}";
            var _headKey = $"{RingBufferProducer.KeyPrefix}:{topic}:__head";
            var idKey = $"{RingBufferProducer.KeyPrefix}:{topic}:__mid";

            _script = LuaScript.Prepare(File.ReadAllText("RingBuffer/consume.lua")).Load(server);
            db.StringSet(_consumerIdKey, NotStarted);
        }

        //public void Start()
        //{
        //    BeginConsuming();
        //}

        //private void BeginProcessing(IObserver<RedisValue> observer)
        //{
        //    if (_running)
        //        throw new InvalidOperationException("Already processing");

        //    _running = true;
        //    Task.Run(() =>
        //    {
        //        while (!_bc.IsCompleted && _running)
        //        {
        //            string data = null;
        //            try
        //            {
        //                data = _bc.Take();
        //            }
        //            catch (InvalidOperationException) { }
        //            if (data != null)
        //            {
        //                observer.OnNext(data);
        //            }
        //        }
        //    });
        //}

        private IObservable<RedisValue> _observable = null;
        public IObservable<RedisValue> AsObservable()
        {
            if (_observable == null)
            {
                _observable = Observable.Create((IObserver<RedisValue> observer) =>
               {
                   _running = true;
                   //BeginProcessing(observer);
                   BeginConsuming(observer);
                   return Disposable.Create(() => { _running = false; });
               });
            }
            return _observable;
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
                        var result = await _db.ScriptEvaluateAsync(_script, new { Size = Size, Topic = Topic, SubscriptionId = SubscriptionId, MaxReadSize = MaxReadSize }).ConfigureAwait(false);
                        if (!result.IsNull)
                        {
                            if (result.ToString() == RingBufferCode.AT_HEAD)
                            {
                                Thread.Sleep(20);
                                continue;
                            }

                            if (result.ToString() == RingBufferCode.PRODUCER_NOT_STARTED)
                            {
                                Thread.Sleep(20);
                                continue;
                            }

                            if (result.ToString() == RingBufferCode.CONSUMER_SYNCED)
                            {
                                Thread.Sleep(20);
                                continue;
                            }

                            if (result.ToString() == RingBufferCode.STARTED)
                            {
                                Thread.Sleep(20);
                                continue;
                            }

                            var range = (RedisValue[])result;
                            var messageCount = range.Length - 3;
                            var id = long.Parse(range[messageCount]);

                            if (lastId != -1 && (lastId + messageCount) != id)
                                throw new MessageLostException();

                            lastId = id;

                            Position = long.Parse(range[messageCount + 1]);
                            Head = long.Parse(range[messageCount + 2]);

                            for (var i = 0; i < messageCount; i++)
                                observer.OnNext(range[i]);

                            //send ack
                            await _db.StringSetAsync(_consumerIdKey, Position);
                        }
                        else
                        {
                            Thread.Sleep(20);
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
