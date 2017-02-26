using StackExchange.Redis;

namespace RedisLib2
{
    public struct Message
    {
        public string Key;
        public RedisValue Value;
        public long MessageId;
    }
}
