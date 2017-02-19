using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Dynamic;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleApplication2
{
    public class RedisQueue {
        private int _batchSize = 500;
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
                    catch(Exception e)
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

    class Program
    {
        static void StartConsumer(ConnectionMultiplexer cm, string q)
        {
            var queue = new RedisQueue(cm.GetDatabase(), cm.GetServer("localhost:6379"), q);

            Stopwatch sw = new Stopwatch();
            sw.Start();
            long? last = null;
            long b = 0;

            Console.WriteLine("starting " + q);

            queue.Subscribe(a => {
                dynamic data = JsonConvert.DeserializeObject<ExpandoObject>(a);
                if (last != null && data.Count - 1 != last)
                {

                }
                b++;
                if (sw.ElapsedMilliseconds > 1000)
                {
                    var latency = (DateTime.UtcNow - (DateTime)data.Time);
                    Console.WriteLine(q + "    " + Math.Round(latency.TotalMilliseconds, 0) + "     " + ((double)b * 1000 / (double)sw.ElapsedMilliseconds) + "   " + queue.Length + "                   ");
                    sw.Reset();
                    sw.Start();
                    b = 0;
                }
                last = data.Count;
            });
        }

        static void Main(string[] args)
        {
            ConnectionMultiplexer cm = ConnectionMultiplexer.Connect("localhost");

           StartConsumer(cm, "test1");
           //StartConsumer(cm, "test2");
            //StartConsumer(cm, "test3");
            //StartConsumer(cm, "test4");

            Console.ReadLine();
        }
    }
}
