using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace ConsoleApplication1
{
    class Program
    {
        static void Main(string[] args)
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            redis.GetDatabase().KeyDelete("test1");
            redis.GetDatabase().KeyDelete("test2");
            redis.GetDatabase().KeyDelete("test3");
            redis.GetDatabase().KeyDelete("test4");

            const string Script = @"redis.call('rpush', @key1, @value);
                                    redis.call('rpush', @key2, @value)";
            var prepared = LuaScript.Prepare(Script);
            var loaded = prepared.Load(redis.GetServer("localhost:6379"));

            long c = 0;
            long d = 10;

            Stopwatch sw = new Stopwatch();

            var persec = 10000;
            
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
                    //redis.GetDatabase().ListRightPushAsync("test2", payload);
                    //redis.GetDatabase().ListRightPushAsync("test3", payload);
                    //redis.GetDatabase().ListRightPushAsync("test4", payload);
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
