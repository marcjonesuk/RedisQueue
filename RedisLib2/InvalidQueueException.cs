using System;

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
}
