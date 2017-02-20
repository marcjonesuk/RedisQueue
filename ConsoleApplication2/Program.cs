using Newtonsoft.Json;
using RedisLib2;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Data.OleDb;
using System.Diagnostics;
using System.Dynamic;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleApplication2
{
    class Program
    {
        static void StartConsumer(ConnectionMultiplexer cm, string q)
        {
            //DDBRDS001.spreadex.com:6381,password=DEV_bc7859c63ce32c5f6636717d9068f234bf4095eaeeff86b08d480396648bfe21
            //var server = cm.GetServer("DDBRDS001.spreadex.com:6381");
            var server = cm.GetServer("localhost:6379");

            var queue = new RedisQueue(cm.GetDatabase(), server, q);

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

        static void StartCircularConsumer(ConnectionMultiplexer cm, string q)
        {
            //DDBRDS001.spreadex.com:6381,password=DEV_bc7859c63ce32c5f6636717d9068f234bf4095eaeeff86b08d480396648bfe21
            //var server = cm.GetServer("DDBRDS001.spreadex.com:6381");
            var server = cm.GetServer("localhost:6379");
            var queue = new RedisCircularBuffer(cm.GetDatabase(), server, 1000000, q, "consumer1");

            long old = -1;
            bool hadone = false;

            Stopwatch sw = new Stopwatch();
            sw.Start();

            var count = 0;
            var c2 = 0;

            queue.Subscribe((a) =>
            {
                count++;
                c2++;
                var num = long.Parse(a);

                if (hadone && num != old + 1)
                    Console.WriteLine("error");

                old = num;
                hadone = true;
                //Thread.Sleep(1);
                if (sw.ElapsedMilliseconds > 1000)
                {
                    var amount = (count * 1000) / sw.ElapsedMilliseconds;
                    Console.WriteLine("\r" + amount + "             " + queue.LocalQueueSize);
                    count = 0;
                    sw.Reset();
                    sw.Start();
                }

                if (c2 % 100000 == 0)
                {
                    Console.WriteLine(c2);
                    Console.WriteLine(a);
                }
            });
        }

        static void Main(string[] args)
        {
            Thread.Sleep(2000);
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
