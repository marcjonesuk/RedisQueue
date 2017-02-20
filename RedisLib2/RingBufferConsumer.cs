using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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

        public long Size
        {
            get; private set;
        }

        public RingBufferConsumer(IDatabase db, IServer server, string topic, int size, string consumerId)
        {
            Topic = topic;
            Size = size;
            _db = db;
            var _key = $"{RingBufferProducer.KeyPrefix}:{topic}";
            var _consumerIdKey = $"{RingBufferProducer.KeyPrefix}:{topic}:{consumerId}";
            var _headKey = $"{RingBufferProducer.KeyPrefix}:{topic}:__head";
            var idKey = $"{RingBufferProducer.KeyPrefix}:{topic}:__mid";

            string initScript = @"";

            string pullScript = @"local head = tonumber(redis.call('GET', '" + _headKey + @"'))
                                        if head == " + RingBufferProducer.Paused + @" then
                                            return
                                        end
                                        local position = tonumber(redis.call('GET', '" + _consumerIdKey + @"'))


redis.call('SET', 'oldpos', position)
redis.call('SET', 'oldhead', head)

                                        if position == " + NotStarted + @" then
                                            redis.call('SET', '" + _consumerIdKey + @"', head)
                                            return
                                        end
                                        if position == head then
                                            return
                                        end
                                        if position < head then
                                            redis.call('SET', '" + _consumerIdKey + @"', head)
                                            if head == " + (size - 1) + @" then
                                                redis.call('SET', '" + _consumerIdKey + @"', -1)
                                            end
                                            local table = {}
                                            table[1] = redis.call('HGET', '" + idKey + @"', position + 1)
                                            local index = 2
                                            for i=position + 1, head do
                                                table[index] = redis.call('HGET', '" + _key + @"', i)
                                                index = index + 1
                                            end
                                            return table
                                        end
                                        if position > head then
                                            redis.call('SET', '" + _consumerIdKey + @"', -1)
                                            local table = {}
                                            table[1] = redis.call('HGET', '" + idKey + @"', position + 1)
                                            local index = 2
                                            for i=position + 1,  " + (size - 1) + @" do
                                                table[index] = redis.call('HGET', '" + _key + @"', i)
                                                index = index + 1
                                            end
                                            return table
                                        end";

            _script = LuaScript.Prepare(pullScript).Load(server);
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
                        var result = await _db.ScriptEvaluateAsync(_script).ConfigureAwait(false);
                        if (!result.IsNull)
                        {
                            var range = (RedisValue[])result;

                                if (long.Parse(range[0]) != long.Parse(range[1]))
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
