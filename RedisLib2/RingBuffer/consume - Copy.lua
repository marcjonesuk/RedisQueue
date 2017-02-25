local head = tonumber(redis.call('GET', '__ringbuffer:' .. @Topic .. ':__head'))

--need to handle null value for head here!!
if head == nil then
	return 'E'
end

--producer hasnt started yet so do nothing
if head == -2 then
	return 'P'
end

local current = tonumber(redis.call('GET', '__ringbuffer:' .. @Topic .. ':' .. @SubscriptionId))

--consumer has just started up so sync to head position
if current == -2 then
	redis.call('SET', '__ringbuffer:' .. @Topic .. ':' .. @SubscriptionId, head)
	return 'S'
end 

--consumer is at the latest message so do nothing
if current == head then
	return 'H'
end

local result = {}
local resultIndex = 0

local continue = true
local lastRead = current

while continue do
	current = current + 1
	resultIndex = resultIndex + 1

	--caught up with the head
	if current > head then
		continue = false
	end
	
	--read the maximum allowed messages so return to allow other consumers to read
	if resultIndex > @MaxReadSize then
		continue = false
	end 

	--reached the last item in the buffer so wrap around to the start
	if current == @Size then 
		current = 0
	end

	if continue then
		result[resultIndex] = redis.call('HGET', '__ringbuffer:' .. @Topic, current)
		lastRead = current
	end
end

table.insert(result, redis.call('HGET', '__ringbuffer:' .. @Topic .. ':__id', lastRead))
table.insert(result, lastRead)
table.insert(result, head)

if @ServerAck == 1 then
	redis.call('SET', '__ringbuffer:' .. @Topic .. ':' .. @SubscriptionId, lastRead)
end
return result
