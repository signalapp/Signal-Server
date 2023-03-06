-- The script encapsulates the logic of a token bucket rate limiter.
-- Two types of operations are supported: 'check-only' and 'use-if-available' (controlled by the 'useTokens' arg).
-- Both operations take in rate limiter configuration parameters and the requested amount of tokens.
-- Both operations return 0, if the rate limiter has enough tokens to cover the requested amount,
-- and the deficit amount otherwise.
-- However, 'check-only' operation doesn't modify the bucket, while 'use-if-available' (if successful)
-- reduces the amount of available tokens by the requested amount.

local bucketId = KEYS[1]

local bucketSize = tonumber(ARGV[1])
local refillRatePerMillis = tonumber(ARGV[2])
local currentTimeMillis = tonumber(ARGV[3])
local requestedAmount = tonumber(ARGV[4])
local useTokens = ARGV[5] and string.lower(ARGV[5]) == "true"

local tokenBucketJson = redis.call("GET", bucketId)
local tokenBucket
local changesMade = false

if tokenBucketJson then
    tokenBucket = cjson.decode(tokenBucketJson)
else
    tokenBucket = {
        ["bucketSize"] = bucketSize,
        ["leakRatePerMillis"] = refillRatePerMillis,
        ["spaceRemaining"] = bucketSize,
        ["lastUpdateTimeMillis"] = currentTimeMillis
    }
end

-- this can happen if rate limiter configuration has changed while the key is still in Redis
if tokenBucket["bucketSize"] ~= bucketSize or tokenBucket["leakRatePerMillis"] ~= refillRatePerMillis then
    tokenBucket["bucketSize"] = bucketSize
    tokenBucket["leakRatePerMillis"] = refillRatePerMillis
    changesMade = true
end

local elapsedTime = currentTimeMillis - tokenBucket["lastUpdateTimeMillis"]
local availableAmount = math.min(
    tokenBucket["bucketSize"],
    math.floor(tokenBucket["spaceRemaining"] + (elapsedTime * tokenBucket["leakRatePerMillis"]))
)

if availableAmount >= requestedAmount then
    if useTokens then
        tokenBucket["spaceRemaining"] = availableAmount - requestedAmount
        tokenBucket["lastUpdateTimeMillis"] = currentTimeMillis
        changesMade = true
    end
    if changesMade then
        local tokensUsed = tokenBucket["bucketSize"] - tokenBucket["spaceRemaining"]
        -- Storing a 'full' bucket is equivalent of not storing any state at all
        -- (in which case a bucket will be just initialized from the input configs as a 'full' one).
        -- For this reason, we either set an expiration time on the record (calculated to let the bucket fully replenish)
        -- or we just delete the key if the bucket is full.
        if tokensUsed > 0 then
            local ttlMillis = math.ceil(tokensUsed / tokenBucket["leakRatePerMillis"])
            redis.call("SET", bucketId, cjson.encode(tokenBucket), "PX", ttlMillis)
        else
            redis.call("DEL", bucketId)
        end
    end
    return 0
else
    return requestedAmount - availableAmount
end
