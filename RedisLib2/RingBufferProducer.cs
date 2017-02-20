using StackExchange.Redis;
using System.Threading.Tasks;

namespace RedisLib2
{
    public class RingBufferProducer
    {
        public long _count;
        public const long Paused = -1;
        public const string KeyPrefix = "__ringbuffer";

        private string _key;
        private string _headKey;
        private IDatabase _db;
        private LoadedLuaScript _script;

        public RingBufferProducer(IDatabase db, IServer server, string topic, int size)
        {
            _db = db;
            _key = $"{KeyPrefix}:{topic}";
            _headKey = $"{KeyPrefix}:{topic}:__head";
            var idKey = $"{KeyPrefix}:{topic}:__mid";

            Size = size;

            string script = @"  local head = tonumber(redis.call('INCR', '" + _headKey + @"'))
                                if head == " + size + @" then
                                    head = 0
                                    redis.call('SET', '" + _headKey + @"', 0)
                                end
                                redis.call('HSET', '" + _key + @"', head, @value)
                                redis.call('HSET', '" + idKey + @"', head, @id)";

            var prepared = LuaScript.Prepare(script);
            _script = prepared.Load(server);

            //clear the ringbuffer - should make this optional
            Clear();
        }

        public void Clear()
        {
            _db.KeyDelete(_key);
            _db.StringSet(_headKey, Paused);
        }

        public Task Publish(RedisValue value)
        {
            var t = _db.ScriptEvaluateAsync(_script, new { @id = _count.ToString().PadLeft(20, '0'), @value = value });
            _count++;
            return t;
        }

        public long Size { get; private set; }
    }
}