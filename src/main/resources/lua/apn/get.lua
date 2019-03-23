-- keys: pending (KEYS[1])
-- argv: max_time (ARGV[1]), limit (ARGV[2])

local hgetall = function (key)
    local bulk = redis.call('HGETALL', key)
    local result = {}
    local nextkey
    for i, v in ipairs(bulk) do
        if i % 2 == 1 then
            nextkey = v
        else
            result[nextkey] = v
        end
    end
    return result
end

local getNextInterval = function(interval)
    if interval < 20000 then
        return 20000
    end

    if interval < 40000 then
        return 40000
    end

    if interval < 80000 then
        return 80000
    end

    if interval < 160000 then
        return 160000
    end

    if interval < 600000 then
        return 600000
    end

    if interval < 1800000 then
        return 1800000
    end

    return 3600000
end


local results  = redis.call("ZRANGEBYSCORE", KEYS[1], 0, ARGV[1], "LIMIT", 0, ARGV[2])
local collated = {}

if results and next(results) then
    for i, name in ipairs(results) do
        local pending      = hgetall(name)
        local lastInterval = pending["interval"]

        if lastInterval == nil then
            lastInterval = 0
        end

        local nextInterval = getNextInterval(tonumber(lastInterval))

        redis.call("HSET", name, "interval", nextInterval)
        redis.call("ZADD", KEYS[1], tonumber(ARGV[1]) + nextInterval, name)

        collated[i] = pending["account"] .. ":" .. pending["device"]
    end
end

return collated


