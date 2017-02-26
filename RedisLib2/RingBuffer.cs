using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RedisLib2
{
    public class InvalidQueueException : Exception
    {
        public InvalidQueueException(string message) : base(message)
        {
        }

        public InvalidQueueException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }

    public class RingBuffer
    {
        public class DbKeys
        {
            public string DataHash { get; internal set; }
            public string HeadString { get; internal set; }
            public string MessageKeyHash { get; internal set; }
            public string LengthString { get; internal set; }

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

        private RingBuffer(IServer server, IDatabase database, string name, long length)
        {
            Server = server;
            Database = database;
            Name = name;
            Length = length;
            DbKey = new DbKeys(GlobalKeyPrefix, name);
        }

        public const long NotStartedValue = -1;
        public const long StartAtHead = -2;

        /// <summary>
        /// Gets or creates a ringbuffer from the given database.
        /// If the named ringbuffer already exists it will be read back from the database.  Otherwise it will create a new ringbuffer with the specified length.
        /// Resizing ringbuffers is not supported therefore this will throw an InvalidringbufferException if the ringbuffer exists but the given size does not match. 
        /// </summary>
        /// <param name="server">The Redis server</param>
        /// <param name="database">The Redis database</param>
        /// <param name="name">The desired name of the ringbuffer</param>
        /// <param name="length">The desired or expected ringbuffer length, must be greater than zero. For existing ringbuffers this must match the current length.</param>
        /// <returns></returns>
        public static async Task<RingBuffer> GetOrCreateAsync(IServer server, IDatabase database, string name, long length)
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
        private static async Task<RingBuffer> CreateAsync(IServer server, IDatabase database, string name, long length)
        {
            var q = new RingBuffer(server, database, name, length);
            var transaction = database.CreateTransaction();
            transaction.StringSetAsync(q.DbKey.HeadString, NotStartedValue);
            transaction.StringSetAsync(q.DbKey.LengthString, length);
            transaction.KeyDeleteAsync(q.DbKey.DataHash);
            var s = await transaction.ExecuteAsync();

            if (!s)
                throw new Exception("Create failed");

            return q;
        }

        private static async Task<RingBuffer> ReadAsync(IServer server, IDatabase database, string name, long length)
        {
            var q = new RingBuffer(server, database, name, length);

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

        /// <summary>
        /// Deletes a ringbuffer from the given database.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public static async Task DeleteAsync(IDatabase database, string name)
        {
            var q = new RingBuffer(null, database, name, 0);
            await database.KeyDeleteAsync(q.DbKey.DataHash);
            await database.KeyDeleteAsync(q.DbKey.HeadString);
            await database.KeyDeleteAsync(q.DbKey.LengthString);
        }

        /// <summary>
        /// Creates a producer for the ringbuffer.
        /// </summary>
        /// <returns></returns>
        public Producer CreateProducer()
        {
            return new Producer(this);
        }

        /// <summary>
        /// Creates a consumer for the ringbuffer. Each consumer has a subscription id which is random by default but can be specified as 
        /// part of the ConsumerOptions parameter. This is required if you want consumers to continue processing from the next message after a restart
        /// or crash
        /// </summary>
        /// <param name="options">Custom configuration options for the consumer. The default values will be used if this parameter is omitted</param>
        /// <returns></returns>
        public Consumer CreateConsumer(ConsumerOptions options = null)
        {
            return new Consumer(this, options);
        }

        public Task<long> GetHeadPosition()
        {
            return Database.StringGetAsync(DbKey.HeadString).ContinueWith(t => long.Parse(t.Result));
        }
    }
}
