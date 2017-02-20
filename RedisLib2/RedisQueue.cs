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
