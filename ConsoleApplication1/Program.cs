using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using RedisLib2;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Text;
using System.Dynamic;
using System.Reactive.Linq;

namespace ConsoleApplication1
{
    class Program
    {
        public static void Circular(ConnectionMultiplexer redis)
        {
            RingBuffer.DeleteAsync(redis.GetDatabase(), "testbuffer").Wait();
            var queue = RingBuffer.GetOrCreateAsync(redis.GetServer("localhost:6379"), redis.GetDatabase(), "testbuffer", 1000000).Result;
            var producer = queue.CreateProducer();

            var batch = 10;
            var numpros = 1;
            long c = -1;

            for (var p = 0; p <= numpros; p++)
            {
                //Thread t = new Thread(async (s) => 
                //{
                var p2 = p;
                long lastsent = 0;
                var count = 0;

                Stopwatch sw = new Stopwatch();
                sw.Start();

                while (true)
                {
                    List<Task> ts = new List<Task>();
                    for (var i = 0; i < batch; i++)
                    {
                        var d = Interlocked.Increment(ref c);
                        Interlocked.Increment(ref count);
                        var payload = JsonConvert.SerializeObject(new { Producer = p2, Time = DateTime.UtcNow.Ticks, Count = d, Padding = Enumerable.Range(0, 0) });
                        ts.Add(producer.Publish("key1", Encoding.UTF8.GetBytes(payload)));
                        
                        if (sw.ElapsedMilliseconds > 1000)
                        {
                            sw.Stop();
                            var rate = ((double)count * 1000) / sw.ElapsedMilliseconds;
                            Console.WriteLine($"\r{rate}");
                            count = 0;
                            sw.Reset();
                            sw.Start();
                        }
                    }

                    Task.WaitAll(ts.ToArray());
                    ts.Clear();

                    //Thread.Sleep(100);
                    //var sleep = (int)Math.Max(1000 - sw.ElapsedMilliseconds, 0);
                    //Thread.Sleep(sleep);
                    //if (sleep == 0)
                    //    Console.WriteLine("Missed");
                }
                //});
                //t.Start();
            }

            Console.ReadLine();
        }

        static void Main(string[] args)
        {
            //ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("DDBRDS001.spreadex.com:6381,password=DEV_bc7859c63ce32c5f6636717d9068f234bf4095eaeeff86b08d480396648bfe21");
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost:6379");

            Task.Run(() => { Circular(redis); });

            Console.ReadLine();
        }
    }
}
