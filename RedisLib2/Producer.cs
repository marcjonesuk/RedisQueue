using StackExchange.Redis;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace RedisLib2
{
    public class Producer
    {
        public RingBuffer Queue { get; private set; }
        private LoadedLuaScript _script;

        internal Producer(RingBuffer queue)
        {
            Queue = queue;
            _script = LuaScript.Prepare(ScriptPreprocessor(File.ReadAllText("Scripts/publish.lua"))).Load(Queue.Server);
        }

        private string ScriptPreprocessor(string script)
        {
            script = script.Replace("@HeadKey", $"{Queue.DbKey.HeadString}");
            script = script.Replace("@DataKey", $"{Queue.DbKey.DataHash}");
            script = script.Replace("@KeyKey", $"{Queue.DbKey.MessageKeyHash}");
            script = script.Replace("@Length", $"'{Queue.Length}'");
            return script;
        }

        public Task Publish(string key, RedisValue value)
        {
            return Queue.Database.ScriptEvaluateAsync(_script, new Message() { Key = key, Value = value });
        }
    }
}

