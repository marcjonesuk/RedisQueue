local result = redis.call('lrange', @key, 0, @batchSize - 1)
redis.call('ltrim', @key, @batchSize, -1)
return result
