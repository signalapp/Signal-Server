-- Releases a message persister's claim to a Redis node when a persist-to-DynamoDB run has finished

local nodeClaimKey = KEYS[1] -- simple string key whose presence indicates a claim on a node
local persisterId  = ARGV[1] -- a string identifying the persister that claimed the node

local claimedByInstanceId = redis.call("GET", nodeClaimKey)

if persisterId == claimedByInstanceId then
    redis.call("DEL", nodeClaimKey)
end
