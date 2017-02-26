using System;

namespace RedisLib2
{
    public class ConsumerOptions
    {
        public AckMode AckMode { get; set; }
        public int BufferSize { get; set; }
        public int MaxReadSize { get; set; }
        public string SubscriptionId { get; set; }
        public TimeSpan RetainPeriod { get; set; }

        public ConsumerOptions()
        {
            AckMode = AckMode.Batch;
            BufferSize = 1000;
            MaxReadSize = 1000;
            SubscriptionId = Guid.NewGuid().ToString();
        }
    }
}
