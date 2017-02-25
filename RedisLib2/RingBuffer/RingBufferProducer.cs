using StackExchange.Redis;
using System.IO;
using System.Threading.Tasks;

namespace RedisLib2
{
    public class RingBufferProducer
    {
        public const long NotStarted = -2;
        public const string KeyPrefix = "__ringbuffer";

        private string _key;
        private string _headKey;
        private string _idKey;
        private IDatabase _db;
        private LoadedLuaScript _publish;

        public string Topic { get; private set; }

        public RingBufferProducer(IDatabase db, IServer server, string topic, int size)
        {
            Topic = topic;
            _db = db;
            _key = $"{KeyPrefix}:{topic}";
            _headKey = $"{KeyPrefix}:{topic}:__head";
            _idKey = $"{KeyPrefix}:{topic}:__nextid";

            Size = size;

            _publish = LuaScript.Prepare(ScriptPreprocessor(File.ReadAllText("RingBuffer/publish.lua"))).Load(server);
            //clear the ringbuffer - should make this optional
            Clear();
        }

        public void Clear()
        {
            _db.KeyDelete(_key);
            _db.StringSet(_headKey, NotStarted);
            _db.StringSet(_idKey, 0);
        }

        private string ScriptPreprocessor(string script)
        {
            script = script.Replace("@Size", $"{Size}");
            script = script.Replace("@Topic", $"'{Topic}'");
            return script;
        }

        public Task Publish(RedisValue value)
        {
            return _db.ScriptEvaluateAsync(_publish, new PublishRequest() { Value = value });
        }

        public Task<long> Publish2(RedisValue value)
        {
            return _db.ScriptEvaluateAsync(_publish, new PublishRequest() { Value = value }).ContinueWith(t => long.Parse((string)t.Result));
        }

        public Task Publish(string key, RedisValue value)
        {
            return _db.ScriptEvaluateAsync(_publish, new PublishRequest() { Value = value});
        }

        public long Size { get; private set; }

        private struct PublishRequest
        {
            public string Key;
            public RedisValue Value;
        }
    }
}