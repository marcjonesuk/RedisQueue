local head = tonumber(redis.call('GET', @HeadKey))

if head == nil then
	return 'E'
end

--producer hasnt started yet so do nothing
if head == -2 then
	return 'P'
end

local current = tonumber(redis.call('GET', @SubscriptionKey))

--consumer has just started up so sync to head position
if current == -2 then
	redis.call('SET', @SubscriptionKey, head)
	return 'S'
end 

--consumer is at the latest message so do nothing
if current == head then
	return 'H'
end

--need to check this is correct
if current <= (head - @Length - @MaxReadSize) then
	return 'L'
end

local result = {}
local resultIndex = 0
current = current + 1
local lastRead = current
local continue = true

while continue do
	table.insert(result, redis.call('HGET', @KeyKey, current % @Length))
	table.insert(result, redis.call('HGET', @DataKey, current % @Length))
	lastRead = current

	current = current + 1
	resultIndex = resultIndex + 1

	--caught up with the head
	if current > head then
		continue = false
	end
	
	--read the maximum allowed messages so return to allow other consumers to read
	if resultIndex == @MaxReadSize then
		continue = false
	end 
end

table.insert(result, lastRead)
table.insert(result, head)

if @ServerAck == 1 then
	redis.call('SET', @SubscriptionKey, lastRead)
end
return result
