using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RedisLib2
{
    public class RedisCircularBuffer
    {
        private IDatabase _db;
        private int _size;
        private LoadedLuaScript _push;
        private LoadedLuaScript _pull;
        private string _name;
        private string _consumerName;
        private BlockingCollection<string> _bc = new BlockingCollection<string>();
        private bool _running;

        public long LocalQueueSize
        {
            get { return _bc.Count; }
        }

        public RedisCircularBuffer(IDatabase db, IServer server, int size, string name, string consumerName)
        {
            _size = size;
            _name = name;
            _db = db;
            _consumerName = consumerName;

            const string Script = @"local head = tonumber(redis.call('INCR', @key .. ':head'))
                                    if head == tonumber(@size) then
                                        head = 0
                                        redis.call('SET', @key .. ':head', 0)
                                    end;
                                    redis.call('HSET', @key, head, @value)";

            const string PullScript = @"local head = tonumber(redis.call('GET', @key .. ':head'))
                                        if head == -1 then
                                            return
                                        end
                                        local position = tonumber(redis.call('GET', @key .. ':' .. @consumer))
                                        if position == -2 then
                                            redis.call('SET', @key .. ':' .. @consumer, head)
                                            return
                                        end
                                        if position == head then
                                            return
                                        end
                                        if position < head then
                                            redis.call('SET', @key .. ':' .. @consumer, head)
                                            local table={}
                                            local index = 0
                                            for i=position + 1, head do
                                                index = index + 1
                                                table[index] = redis.call('HGET', @key, i)
                                            end
                                            return table
                                        end
                                        if position > head then
                                            redis.call('SET', @key .. ':' .. @consumer, -1)
                                            local table={}
                                            local index = 0
                                            for i=position + 1, @size do
                                                index = index + 1
                                                table[index] = redis.call('HGET', @key, i)
                                            end
                                            return table
                                        end";

            var prepared = LuaScript.Prepare(Script);
            _push = prepared.Load(server);
            var prepared2 = LuaScript.Prepare(PullScript);
            _pull = prepared2.Load(server);

            if (consumerName == null) //producer
            {
                //create an empty list
                db.KeyDelete(name);
                db.StringSet(_name + ":head", -1);
            }
            else
            {
                db.StringSet(_name + ":" + _consumerName, -2);
            }
        }

        private long head = -1;
        public void Publish(RedisValue value)
        {
            //head++;
            //if (head == _size)
            //    head = 0;

            _db.ScriptEvaluateAsync(_push, new { key = _name, @value = value, @size = _size });
        }

        public void Subscribe(Action<RedisValue> action)
        {
            if (_running)
                throw new InvalidOperationException("Already processing queue");

            _running = true;
            Task.Run(() =>
            {
                while (!_bc.IsCompleted)
                {
                    string data = null;
                    try
                    {
                        data = _bc.Take();
                    }
                    catch (InvalidOperationException) { }
                    if (data != null)
                    {
                        action(data);
                    }
                }
            });

            Task.Run(async () =>
            {
                while (_running)
                {
                    try
                    {
                        var result = await _db.ScriptEvaluateAsync(_pull, new { key = _name, consumer = _consumerName, @size = _size }).ConfigureAwait(false);
                        if (!result.IsNull)
                        {
                            var range = (RedisValue[]) result;
                            foreach (var redisValue in range)
                            {
                                _bc.Add(redisValue);
                            }
                        }
                        else
                        {
                            Thread.Sleep(1);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                }
                _bc.CompleteAdding();
            });
        }
    }

    public class RedisQueue
    {
        private int _batchSize = 50;
        private BlockingCollection<string> _bc = new BlockingCollection<string>();
        private LoadedLuaScript loaded;
        private IDatabase _db;
        private string _queue;
        private bool _running;
        public long Length;

        public RedisQueue(IDatabase db, IServer server, string queueName)
        {
            _db = db;
            _queue = queueName;
            const string Script = @"local result = redis.call('lrange', @key, 0, @batchSize - 1)
                                    redis.call('ltrim', @key, @batchSize, -1)
                                    return result";
            var prepared = LuaScript.Prepare(Script);
            loaded = prepared.Load(server);
        }

        public void Subscribe(Action<string> action)
        {
            if (_running)
                throw new InvalidOperationException("Already processing queue");

            _running = true;
            Task.Run(() =>
            {
                while (!_bc.IsCompleted)
                {
                    string data = null;
                    try
                    {
                        data = _bc.Take();
                    }
                    catch (InvalidOperationException) { }
                    if (data != null)
                    {
                        action(data);
                    }
                }
            });

            Task.Run(async () =>
            {
                while (_running)
                {
                    try
                    {
                        var result = await _db.ScriptEvaluateAsync(loaded, new { key = _queue, batchSize = _batchSize }).ConfigureAwait(false);
                        // Length = await _db.ListLengthAsync(_queue).ConfigureAwait(false);
                        var range = (RedisValue[])result;
                        foreach (var r in range)
                            _bc.Add(r);

                        if (range.Length == 0)
                            Thread.Sleep(1);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                }
                _bc.CompleteAdding();
            });
        }

        public void Stop()
        {
            _running = false;
        }
    }
}
