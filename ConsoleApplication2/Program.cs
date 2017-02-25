using Newtonsoft.Json;
using RedisLib2;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Data.OleDb;
using System.Diagnostics;
using System.Dynamic;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleApplication2
{
    class Program
    {
        static void StartCircularConsumer(ConnectionMultiplexer cm, string q)
        {
            //DDBRDS001.spreadex.com:6381,password=DEV_bc7859c63ce32c5f6636717d9068f234bf4095eaeeff86b08d480396648bfe21
            //var server = cm.GetServer("DDBRDS001.spreadex.com:6381");
            var server = cm.GetServer("localhost:6379");

            var total = 0;

            for (int i = 1; i <= 1; i++)
            {
                var ring = new RingBufferConsumer(cm.GetDatabase(), server, q, 100000, "consumer" + i, // + Guid.NewGuid().ToString(), 
                    new ConsumerOptions(AckMode.Process, 50, 50));

                long old = -1;
                bool hadone = false;
                Stopwatch sw = new Stopwatch();
                sw.Start();
                var count = 0;
                var c2 = 0;
                var latency = 0d;

                ring.AsObservable()
                    .Do(m => { }, (Exception e) =>
                    {
                        Console.WriteLine(e);
                        hadone = false;
                    })
                    .Retry()
                    .Subscribe((a) =>
                    {
                        //Thread.Sleep(1);

                        count++;
                        c2++;
                        Interlocked.Increment(ref total);
                        dynamic x = JsonConvert.DeserializeObject<ExpandoObject>(a);
                        var num = x.Count; // long.Parse(a);
                        if (hadone && num != old + 1)
                            Console.WriteLine("error");

                        old = num;
                        hadone = true;
                        //Thread.Sleep(1);

                        //dynamic x = JsonConvert.DeserializeObject<ExpandoObject>(a);
                        latency += ((TimeSpan)(DateTime.UtcNow - x.Time)).TotalMilliseconds;

                        if (sw.ElapsedMilliseconds > 1000)
                        {
                            //dynamic x = JsonConvert.DeserializeObject<ExpandoObject>(a);

                            var amount = (count * 1000) / sw.ElapsedMilliseconds;
                            Console.WriteLine($"{Math.Round(latency / count, 2)}  c" + i + "   " + amount + "   " + a + " " + total);
                            count = 0;
                            latency = 0;
                            sw.Reset();
                            sw.Start();
                        }
                    }, (Exception e) =>
                    {
                        Console.WriteLine(e);
                        hadone = false;
                    });
            }
        }

        static void Main(string[] args)
        {
            Thread.Sleep(5000);
            //ConnectionMultiplexer cm = ConnectionMultiplexer.Connect("DDBRDS001.spreadex.com:6381,password=DEV_bc7859c63ce32c5f6636717d9068f234bf4095eaeeff86b08d480396648bfe21");
            ConnectionMultiplexer cm = ConnectionMultiplexer.Connect("localhost:6379");

            StartCircularConsumer(cm, "testbuffer");

            //StartConsumer(cm, "test1");
            //StartConsumer(cm, "test2");
            //StartConsumer(cm, "test3");
            //StartConsumer(cm, "test4");

            Console.ReadLine();
        }
    }
}
