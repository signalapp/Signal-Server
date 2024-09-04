-- Removes the given recipient view from the shared MRM data. If the only field remaining after the removal is the
-- `data` field, then the key will be deleted

local sharedMrmKeys         = KEYS    -- KEYS: list of all keys in a single slot to update
local recipientViewToRemove = ARGV[1] -- the recipient view to remove from the hash

local keysDeleted = 0

for _, sharedMrmKey in ipairs(sharedMrmKeys) do
    redis.call("HDEL", sharedMrmKey, recipientViewToRemove)
    if redis.call("HLEN", sharedMrmKey) == 1 then
        redis.call("DEL", sharedMrmKey)
        keysDeleted = keysDeleted + 1
    end
end

return keysDeleted
