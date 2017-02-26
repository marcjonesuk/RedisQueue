using Newtonsoft.Json;
using RedisLib2;
using StackExchange.Redis;
using System;
using System.Diagnostics;
using System.Dynamic;
using System.Reactive.Linq;
using System.Text;
using System.Threading;

namespace ConsoleApplication2
{
    class Program
    {
        static void StartCircularConsumer(ConnectionMultiplexer cm)
        {
            var server = cm.GetServer("localhost:6379");
            var q = RingBuffer.GetOrCreateAsync(server, cm.GetDatabase(), "testbuffer", 1000000).Result;

            var total = 0;

            for (int i = 1; i <= 1; i++)
            {
                var consumer = q.CreateConsumer();

                long old = -1;
                bool hadone = false;
                Stopwatch sw = new Stopwatch();
                sw.Start();
                var count = 0;
                var c2 = 0;
                var latency = 0d;

                consumer.Start()
                    .Subscribe((a) =>
                    {
                        try
                        {
                            //Thread.Sleep(1);
                            count++;
                            c2++;
                            Interlocked.Increment(ref total);
                            dynamic x = JsonConvert.DeserializeObject<ExpandoObject>(Encoding.UTF8.GetString(a.Value));
                            var num = x.Count;
                            if (hadone && num != old + 1)
                                Console.WriteLine("error");

                            old = num;
                            hadone = true;
                            //Thread.Sleep(1);
                            latency += (double)(DateTime.UtcNow.Ticks - x.Time) / TimeSpan.TicksPerMillisecond;

                            if (sw.ElapsedMilliseconds > 1000)
                            {
                                sw.Stop();
                                var amount = (count * 1000) / sw.ElapsedMilliseconds;
                                Console.WriteLine($"{Math.Round(latency / count, 2)}  c" + i + "   " + amount + "   " + a + " " + total);
                                count = 0;
                                latency = 0;
                                sw.Reset();
                                sw.Start();
                            }
                        }
                        catch(Exception e)
                        {
                            Console.WriteLine(e);
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

            


            Thread.Sleep(1000);
            //ConnectionMultiplexer cm = ConnectionMultiplexer.Connect("DDBRDS001.spreadex.com:6381,password=DEV_bc7859c63ce32c5f6636717d9068f234bf4095eaeeff86b08d480396648bfe21");
            ConnectionMultiplexer cm = ConnectionMultiplexer.Connect("localhost:6379");

            //var p = RedQ.CreateProducer(cm.GetDatabase(), cm.GetServer(""), "", 1024);
            //p.Publish("", new byte[5]);

            //var consumer = RedQ.CreateConsumer(cm.GetDatabase(), cm.GetServer(""), "testtopic", "sub1");
            //consumer.Start()
            //    .Retry()
            //    .Subscribe(m =>
            //{
            //});

            StartCircularConsumer(cm);

            //StartConsumer(cm, "test1");
            //StartConsumer(cm, "test2");
            //StartConsumer(cm, "test3");
            //StartConsumer(cm, "test4");

            Console.ReadLine();
        }
    }
}
