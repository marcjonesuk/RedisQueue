local head = tonumber(redis.call('INCR', '__ringbuffer:' .. @Topic .. ':__head'))
local id = tonumber(redis.call('INCR', '__ringbuffer:' .. @Topic .. ':__id'))
if head == tonumber(@Size) then
	head = 0
    redis.call('SET', '__ringbuffer:' .. @Topic .. ':head', 0)
end
redis.call('HSET', '__ringbuffer:' .. @Topic, head, @Value)
redis.call('HSET', '__ringbuffer:' .. @Topic .. ':id' , head, id)
