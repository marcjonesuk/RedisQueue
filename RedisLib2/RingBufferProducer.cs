using RedisLib2;
using StackExchange.Redis;
using System;
using System.IO;
using System.Threading.Tasks;

namespace RedisLib2
{
    public struct Message
    {
        public string Key;
        public byte[] Value;
        public long MessageId;
    }

    public class InvalidQueueException : Exception
    {
        public InvalidQueueException(string message) : base(message)
        {
        }

        public InvalidQueueException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }

    public class RedQ
    {
        public class DbKeys
        {
            internal string DataHash { get; set; }
            internal string HeadString { get; set; }
            internal string MessageKeyHash { get; set; }
            internal string LengthString { get; set; }

            public DbKeys(string prefix, string name)
            {
                DataHash = $"{prefix}:{name}";
                HeadString = $"{prefix}:{name}:__head";
                MessageKeyHash = $"{prefix}:{name}:__key";
                LengthString = $"{prefix}:{name}:__length";
            }

            internal string GetSubscriptionKey(string subscriptionId)
            {
                return $"{DataHash}:{subscriptionId}";
            }
        }

        public static class RingBufferResponse
        {
            public static readonly string AT_HEAD = "H";
            public static readonly string PRODUCER_NOT_STARTED = "P";
            public static readonly string CONSUMER_SYNCED = "C";
            public static readonly string STARTED = "S";
            public static readonly string LAPPED = "L";
        }

        public string Name { get; private set; }
        public long Length { get; private set; }
        public IDatabase Database { get; private set; }
        public IServer Server { get; private set; }
        public DbKeys DbKey { get; private set; }
        
        private string GlobalKeyPrefix = "__redq";

        private RedQ(IServer server, IDatabase database, string name, long length)
        {
            Server = server;
            Database = database;
            Name = name;
            Length = length;
            DbKey = new DbKeys(GlobalKeyPrefix, name);
        }

        public const long NotStartedValue = -2;

        /// <summary>
        /// Gets or creates a queue from the given database.
        /// If the named queue already exists it will be read back from the database.  Otherwise it will create a new queue with the specified length.
        /// Resizing queues is not supported therefore this will throw an InvalidQueueException if the queue exists but the given size does not match. 
        /// </summary>
        /// <param name="server">The Redis server</param>
        /// <param name="database">The Redis database</param>
        /// <param name="name">The desired name of the queue</param>
        /// <param name="length">The desired or expected queue length, must be greater than zero. For existing queues this must match the current length.</param>
        /// <returns></returns>
        public static async Task<RedQ> GetOrCreateAsync(IServer server, IDatabase database, string name, long length)
        {
            if (server == null)
                throw new ArgumentNullException(nameof(server));

            if (database == null)
                throw new ArgumentNullException(nameof(database));

            if (database == null)
                throw new ArgumentNullException(nameof(database));

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (length <= 0)
                throw new InvalidQueueException("Length must be greater than 0");

            var queue = await ReadAsync(server, database, name, length);

            if (queue == null)
                queue = await CreateAsync(server, database, name, length);

            return queue;
        }

        //to do: need to move this all over to lua so that we can create it atomically
        private static async Task<RedQ> CreateAsync(IServer server, IDatabase database, string name, long length)
        {
            var q = new RedQ(server, database, name, length);

            var transaction = database.CreateTransaction();
            await transaction.KeyDeleteAsync(q.DbKey.DataHash);
            await transaction.StringSetAsync(q.DbKey.HeadString, NotStartedValue);
            await transaction.StringSetAsync(q.DbKey.LengthString, length);
            await transaction.ExecuteAsync();
            return q;
        }

        private static async Task<RedQ> ReadAsync(IServer server, IDatabase database, string name, long length)
        {
            var q = new RedQ(server, database, name, length);

            var exists = await database.KeyExistsAsync(q.DbKey.HeadString);
            if (exists)
            {
                //resizing isnt supported so throw a InvalidQueueException if size is incorrect
                try
                {
                    string l = await database.StringGetAsync(q.DbKey.LengthString);
                    var serverLength = long.Parse(l);
                    if (length != serverLength)
                        throw new InvalidQueueException($"Expected a queue length of {length}, got {serverLength}");
                }
                catch (Exception e)
                {
                    throw new InvalidQueueException($"Found existing queue {name} but failed to read it back", e);
                }
                return q;
            }
            else
            {
                return null;
            }
        }

        public static Task DeleteAsync(string name)
        {
            return null;
        }

        public RingBufferProducer CreateProducer()
        {
            return new RingBufferProducer(this);
        }

        public RingBufferConsumer CreateConsumer(ConsumerOptions options = null)
        {
            return new RingBufferConsumer(this, options);
        }

        public Task<long> ReadHead()
        {
            return Database.StringGetAsync(DbKey.HeadString).ContinueWith(t => long.Parse(t.Result));
        }
    }

    public class RingBufferProducer
    {
        private LoadedLuaScript _script;
        public RedQ Queue { get; private set; }

        internal RingBufferProducer(RedQ queue)
        {
            Queue = queue;
            _script = LuaScript.Prepare(ScriptPreprocessor(File.ReadAllText("RingBuffer/publish.lua"))).Load(Queue.Server);
        }

        private string ScriptPreprocessor(string script)
        {
            script = script.Replace("@HeadKey", $"{Queue.DbKey.HeadString}");
            script = script.Replace("@DataKey", $"{Queue.DbKey.DataHash}");
            script = script.Replace("@KeyKey", $"{Queue.DbKey.MessageKeyHash}");
            script = script.Replace("@Length", $"'{Queue.Length}'");
            return script;
        }

        public Task Publish(string key, byte[] value)
        {
            return Queue.Database.ScriptEvaluateAsync(_script, new Message() { Key = key, Value = value });
        }
    }
}

