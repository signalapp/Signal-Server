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

local SIZE_FIELD = "s"
local TIME_FIELD = "t"

local changesMade = false
local tokensRemaining
local lastUpdateTimeMillis

local tokensRemainingStr, lastUpdateTimeMillisStr = unpack(redis.call("HMGET", bucketId, SIZE_FIELD, TIME_FIELD))
if tokensRemainingStr and lastUpdateTimeMillisStr then
    tokensRemaining = tonumber(tokensRemainingStr)
    lastUpdateTimeMillis = tonumber(lastUpdateTimeMillisStr)
else
    tokensRemaining = bucketSize
    lastUpdateTimeMillis = currentTimeMillis
end

local elapsedTime = currentTimeMillis - lastUpdateTimeMillis
local availableAmount = math.min(
    bucketSize,
    math.floor(tokensRemaining + (elapsedTime * refillRatePerMillis))
)

if availableAmount >= requestedAmount then
    if useTokens then
        tokensRemaining = availableAmount - requestedAmount
        lastUpdateTimeMillis = currentTimeMillis
        changesMade = true
    end
    if changesMade then
        local tokensUsed = bucketSize - tokensRemaining
        -- Storing a 'full' bucket (i.e. tokensUsed == 0) is equivalent of not storing any state at all
        -- (in which case a bucket will be just initialized from the input configs as a 'full' one).
        -- For this reason, we either set an expiration time on the record (calculated to let the bucket fully replenish)
        -- or we just delete the key if the bucket is full.
        if tokensUsed > 0 then
            local ttlMillis = math.ceil(tokensUsed / refillRatePerMillis)
            redis.call("HSET", bucketId, SIZE_FIELD, tokensRemaining, TIME_FIELD, lastUpdateTimeMillis)
            redis.call("PEXPIRE", bucketId, ttlMillis)
        else
            redis.call("DEL", bucketId)
        end
    end
    return 0
else
    return requestedAmount - availableAmount
end
