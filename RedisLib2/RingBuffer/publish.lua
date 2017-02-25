local head = tonumber(redis.call('INCR', '__ringbuffer:' .. @Topic .. ':__head'))
redis.call('HSET', '__ringbuffer:' .. @Topic, head % @Size, @Value)
