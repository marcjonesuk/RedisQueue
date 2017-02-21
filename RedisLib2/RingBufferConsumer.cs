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
        private const long MaxReadSize = 50;
        public const long NotStarted = -2;
        private IDatabase _db;
        private int _size;
        private LoadedLuaScript _script;
        private string _topic;

        private BlockingCollection<string> _bc = new BlockingCollection<string>(10);
        private bool _running;

        public long LocalQueueSize
        {
            get { return _bc.Count; }
        }

        public string Topic
        {
            get; private set;
        }

        public string ConsumerId
        {
            get; private set;
        }

        public long Size
        {
            get; private set;
        }

        public RingBufferConsumer(IDatabase db, IServer server, string topic, int size, string consumerId)
        {
            Topic = topic;
            Size = size;
            ConsumerId = consumerId;

            _db = db;
            var _key = $"{RingBufferProducer.KeyPrefix}:{topic}";
            var _consumerIdKey = $"{RingBufferProducer.KeyPrefix}:{topic}:{consumerId}";
            var _headKey = $"{RingBufferProducer.KeyPrefix}:{topic}:__head";
            var idKey = $"{RingBufferProducer.KeyPrefix}:{topic}:__mid";

            _script = LuaScript.Prepare(File.ReadAllText("consumer.lua")).Load(server);
            db.StringSet(_consumerIdKey, NotStarted);
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
                long msgId;
                long expectedMsgId = -1;
                while (_running)
                {
                    try
                    {
                        var result = await _db.ScriptEvaluateAsync(_script, new { Size = Size, Topic = Topic, ConsumerId = ConsumerId, MaxReadSize = MaxReadSize }).ConfigureAwait(false);
                        if (!result.IsNull)
                        {
                            if (result.ToString().StartsWith("AT HEAD"))
                            {
                                Thread.Sleep(100);
                                continue;
                            }

                            if (result.ToString() == "PRODUCER NOT STARTED")
                            {
                                Thread.Sleep(100);
                                continue;
                            }

                            if (result.ToString() == "CONSUMER STARED")
                            {
                                Thread.Sleep(100);
                                continue;
                            }

                            var range = (RedisValue[])result;

                            try
                            {
                                if (long.Parse(range[0]) != long.Parse(range[1]))
                                {

                                }
                            }
                            catch (Exception e)
                            {

                            }


                            if (range[range.Length - 1].IsNull)
                            {

                            }

                            if (range[0].ToString().Length != 20)
                                throw new Exception("no id");

                            msgId = long.Parse(range[0]);

                            if (expectedMsgId != -1 && msgId != expectedMsgId)
                                throw new MessageLostException();

                            for (var i = 1; i < range.Length; i++)
                                _bc.Add(range[i]);

                            expectedMsgId = msgId + range.Length - 1;
                            last = range;
                        }
                        else
                        {
                            Thread.Sleep(10);
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
