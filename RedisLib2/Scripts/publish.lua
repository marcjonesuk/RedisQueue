local head = tonumber(redis.call('INCR', '@HeadKey'))
redis.call('HSET', '@DataKey', head % @Length, @Value)

if @Key ~= nil then
	redis.call('HSET', '@KeyKey', head % @Length, @Key)
end
