local head = tonumber(redis.call('INCR', '__ringbuffer:' .. @Topic .. ':__head'))
--local id = tonumber(redis.call('INCR', '__ringbuffer:' .. @Topic .. ':__nextid'))
redis.call('HSET', '__ringbuffer:' .. @Topic, head % @Size, @Value)
--redis.call('HSET', '__ringbuffer:' .. @Topic .. ':__id' , head, id)
