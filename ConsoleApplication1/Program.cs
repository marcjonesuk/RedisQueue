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

namespace ConsoleApplication1
{
    class Program
    {
        public static void Circular(ConnectionMultiplexer redis)
        {
            Stopwatch sw = new Stopwatch();
            //var buffer = new RedisCircularBuffer(redis.GetDatabase(), redis.GetServer("DDBRDS001.spreadex.com:6381"), 1000000, "testbuffer", null);
            var buffer = new RingBufferProducer(redis.GetDatabase(), redis.GetServer("localhost:6379"), "testbuffer", 100000);

            var batch = 10;
            Thread.Sleep(1500);
            var numpros = 1;
            long c = -1;

            for (var p = 0; p <= numpros; p++)
            {
                //Thread t = new Thread(async (s) => 
                //{
                var p2 = p;
                long lastsent = 0;
                while (true)
                {
                    sw.Reset();
                    sw.Start();
                    List<Task> ts = new List<Task>();
                    for (var i = 0; i < batch; i++)
                    {
                        var d = Interlocked.Increment(ref c);
                        var payload = JsonConvert.SerializeObject(new { Producer = p2, Time = DateTime.UtcNow, Count = d, Padding = Enumerable.Range(0, 0) });
                        ts.Add(buffer.Publish(payload));

                        //if (lastsent != 0 && x!= lastsent + 1)
                        //{
                        //}
                        //lastsent = x;
                        if (c % 10000 == 0)
                            Console.WriteLine(c);
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
            Circular(redis);
        }
    }
}
