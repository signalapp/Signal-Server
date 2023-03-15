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

-- while we're migrating from json to redis list key types, there are three possible options for the
-- type of the `bucketId` key: "string" (legacy, json value), "list" (new format), "none" (key not set).
--
-- In the phase 1 of migration, we prepare the script to deal with the phase 2 :) I.e. when phase 2 will be rolling out,
-- it will start writing data in the new format, and the still running instances of the previous version
-- need to be able to know how to read the new format before we start writing it.
--
-- On a separate note -- the reason we're not using a different key is because Redis Lua requires to list all keys
-- as a script input and we don't want to expose this migration to the script users.
--
-- Finally, it's okay to read the "ok" key of the return here because "TYPE" command always succeeds.
local keyType = redis.call("TYPE", bucketId)["ok"]
if keyType == "none" then
    -- if the key is not set, building the object from the configuration
    tokensRemaining = bucketSize
    lastUpdateTimeMillis = currentTimeMillis
elseif keyType == "string" then
    -- if the key is "string", we parse the value from json
    local fromJson = cjson.decode(redis.call("GET", bucketId))
    if bucketSize ~= fromJson.bucketSize or refillRatePerMillis ~= fromJson.leakRatePerMillis then
        changesMade = true
    end
    tokensRemaining = fromJson.spaceRemaining
    lastUpdateTimeMillis = fromJson.lastUpdateTimeMillis
elseif keyType == "hash" then
    -- finally, reading values from the new storage format
    local tokensRemainingStr, lastUpdateTimeMillisStr = unpack(redis.call("HMGET", bucketId, SIZE_FIELD, TIME_FIELD))
    tokensRemaining = tonumber(tokensRemainingStr)
    lastUpdateTimeMillis = tonumber(lastUpdateTimeMillisStr)
    redis.call("DEL", bucketId)
    changesMade = true
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
        -- Storing a 'full' bucket is equivalent of not storing any state at all
        -- (in which case a bucket will be just initialized from the input configs as a 'full' one).
        -- For this reason, we either set an expiration time on the record (calculated to let the bucket fully replenish)
        -- or we just delete the key if the bucket is full.
        if tokensUsed > 0 then
            local ttlMillis = math.ceil(tokensUsed / refillRatePerMillis)
            local tokenBucket = {
                ["bucketSize"] = bucketSize,
                ["leakRatePerMillis"] = refillRatePerMillis,
                ["spaceRemaining"] = tokensRemaining,
                ["lastUpdateTimeMillis"] = lastUpdateTimeMillis
            }
            redis.call("SET", bucketId, cjson.encode(tokenBucket), "PX", ttlMillis)
        else
            redis.call("DEL", bucketId)
        end
    end
    return 0
else
    return requestedAmount - availableAmount
end
