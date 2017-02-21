using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using RedisLib2;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ConsoleApplication1
{
    class Program
    {
        public static void Circular(ConnectionMultiplexer redis)
        {
            Stopwatch sw = new Stopwatch();
            //var buffer = new RedisCircularBuffer(redis.GetDatabase(), redis.GetServer("DDBRDS001.spreadex.com:6381"), 1000000, "testbuffer", null);
            var buffer = new RingBufferProducer(redis.GetDatabase(), redis.GetServer("localhost:6379"), "testbuffer", 1000000);
            //var buffer2 = new RingBufferProducer(redis.GetDatabase(), redis.GetServer("localhost:6379"), "testbuffer", 1000000);

            var persec = 100000;

            var numpros = 2;
            for (var pro = 0; pro < numpros; pro++)
            {
                Task.Run(() =>
                {
                    for (var p = 0; p < 1; p++)
                    {
                        List<Task> t = new List<Task>();
                        Task.Run(() =>
                        {
                            long c = -1;
                            while (true)
                            {
                                sw.Reset();
                                sw.Start();
                                for (var i = 0; i < persec; i++)
                                {
                                    c++;
                                    var payload =
                                        JsonConvert.SerializeObject(
                                            new {Time = DateTime.UtcNow, Count = c, Padding = Enumerable.Range(0, 100)});
                                    t.Add(buffer.Publish(payload));

                                    if (c % 10000 == 0)
                                        Console.WriteLine(c);
                                }

                                Task.WaitAll(t.ToArray());
                                t.Clear();

                                var sleep = (int) Math.Max(1000 - sw.ElapsedMilliseconds, 0);
                                Thread.Sleep(sleep);
                            }
                        });
                    }
                });
            }

            Console.ReadLine();
        }

        static void Main(string[] args)
        {
            //ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("DDBRDS001.spreadex.com:6381,password=DEV_bc7859c63ce32c5f6636717d9068f234bf4095eaeeff86b08d480396648bfe21");
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost:6379");

            Circular(redis);

            redis.GetDatabase().KeyDelete("test1");
            redis.GetDatabase().KeyDelete("test2");
            redis.GetDatabase().KeyDelete("test3");
            redis.GetDatabase().KeyDelete("test4");

            const string Script = @"redis.call('rpush', @key1, @value);
                                    redis.call('rpush', @key2, @value)";

            var prepared = LuaScript.Prepare(Script);
            var loaded = prepared.Load(redis.GetServer("localhost:6379"));
            //var loaded = prepared.Load(redis.GetServer("DDBRDS001.spreadex.com:6381"));

            long c = 0;
            long d = 10;

            Stopwatch sw = new Stopwatch();

            var persec = 500;
            while(true)
            { 
                sw.Reset();
                sw.Start();
                for (var i = 0; i < persec; i++)
                {
                    c++;
                    var payload = JsonConvert.SerializeObject(new { Time = DateTime.UtcNow, Count = c, Padding = Enumerable.Range(0, 10) });
                    //redis.GetDatabase().ScriptEvaluateAsync(loaded, new { key1 = "test1", key2="test2", value = payload });

                    redis.GetDatabase().ListRightPushAsync("test1", payload);
                    redis.GetDatabase().ListRightPushAsync("test2", payload);
                    redis.GetDatabase().ListRightPushAsync("test3", payload);
                    redis.GetDatabase().ListRightPushAsync("test4", payload);
                }

                var sleep = (int)Math.Max(1000 - sw.ElapsedMilliseconds, 0);

                Thread.Sleep(sleep);

                //redis.GetDatabase().ScriptEvaluateAsync(loaded, new { @key = "test", @key2 = "test2", @value = payload });

                //redis.GetSubscriber().PublishAsync("test", payload);

                //if (c % 10000 == 0)
                //    Thread.Sleep(1);

                //if (c % d == 0)
                //{
                //    Thread.Sleep(1);
                //}

                //if (c % 1000 == 0)
                //{
                //    d++;
                //}
            }
        }
    }
}
