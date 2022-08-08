local lastBackgroundNotificationTimestampKey = KEYS[1]
local queueKey = KEYS[2]

local accountDevicePair = ARGV[1]
local currentTimeMillis = tonumber(ARGV[2])
local backgroundNotificationPeriod = tonumber(ARGV[3])

local lastBackgroundNotificationTimestamp = redis.call("GET", lastBackgroundNotificationTimestampKey)
local nextNotificationTimestamp

if (lastBackgroundNotificationTimestamp) then
    nextNotificationTimestamp = tonumber(lastBackgroundNotificationTimestamp) + backgroundNotificationPeriod
else
    nextNotificationTimestamp = currentTimeMillis
end

redis.call("ZADD", queueKey, "NX", nextNotificationTimestamp, accountDevicePair)
