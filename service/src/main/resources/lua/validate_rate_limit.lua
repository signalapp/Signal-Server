local bucketId = KEYS[1]

local bucketSize = tonumber(ARGV[1])
local leakRatePerMillis = tonumber(ARGV[2])
local currentTimeMillis = tonumber(ARGV[3])
local amount = tonumber(ARGV[4])

local leakyBucket

if redis.call("EXISTS", bucketId) == 1 then
    leakyBucket = cjson.decode(redis.call("GET", bucketId))
else
    leakyBucket = {
        bucketSize = bucketSize,
        leakRatePerMillis = leakRatePerMillis,
        spaceRemaining = bucketSize,
        lastUpdateTimeMillis = currentTimeMillis
    }
end

local elapsedTime = currentTimeMillis - leakyBucket["lastUpdateTimeMillis"]
local updatedSpaceRemaining = math.min(leakyBucket["bucketSize"], math.floor(leakyBucket["spaceRemaining"] + (elapsedTime * leakyBucket["leakRatePerMillis"])))

if updatedSpaceRemaining >= amount then
    leakyBucket["spaceRemaining"] = updatedSpaceRemaining - amount
    redis.call("SET", bucketId, cjson.encode(leakyBucket))
    return true
else
    return false
end
