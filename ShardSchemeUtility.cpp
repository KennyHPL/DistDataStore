#include "ShardSchemeUtility.h"

#include <cassert>

using namespace std;

namespace
{

/// Creates a shard scheme with a version.
ShardScheme
createShardScheme(int version, size_t numShards, vector<string> allAddresses)
{
    assert(numShards > 0);

    ShardScheme newScheme(version);

    size_t numNodesPerShard = allAddresses.size() / numShards;
    size_t numNodesLastShard = allAddresses.size() - numNodesPerShard * (numShards - 1);

    size_t hashBase = (size_t)(-1) / numShards;

    auto addNodesToShard = [&allAddresses](ShardInfo &shard, size_t numNodes) {
        for (size_t nodeIdx = 0; nodeIdx < numNodes; ++nodeIdx) {
            shard.addNode(allAddresses.back());
            allAddresses.pop_back();
        }
    };

    for (size_t shardId = 0; shardId < numShards - 1; ++shardId) {
        ShardInfo shard(hashBase * (shardId + 1));
        addNodesToShard(shard, numNodesPerShard);
        newScheme.addShard(shard);
    }

    // Last shard needs fewer nodes.
    {
        ShardInfo shard(hashBase * numShards);
        addNodesToShard(shard, numNodesLastShard);
        newScheme.addShard(shard);
    }

    return newScheme;
}
} // namespace

namespace ShardSchemeUtility
{

ShardScheme
createInitialShardScheme(size_t numShards, vector<string> allAddresses)
{
    return createShardScheme(0, numShards, allAddresses);
}

ShardScheme
createNewShardScheme(const ShardScheme &old, size_t numShards)
{
    // TODO: This should try to minimize the amount of data that is moved.

    vector<string> allAddresses;

    for (size_t shardId = 0; shardId < old.getNumShards(); ++shardId) {
        const ShardInfo &shard = old.getShardInfo(shardId);
        const auto &nodeSet = shard.getNodeSet();
        allAddresses.insert(allAddresses.end(), nodeSet.begin(), nodeSet.end());
    }

    return createShardScheme(old.version() + 1, numShards, allAddresses);
}

ShardScheme
addNodeToScheme(const ShardScheme &old, const std::string &address)
{
    // Inserts address into least populated shard.

    size_t candidateShard = 0;
    size_t candidateSize = old.getShardInfo(candidateShard).getNumNodes();

    for (size_t shardId = 1; shardId < old.getNumShards(); ++shardId) {
        size_t size = old.getShardInfo(shardId).getNumNodes();

        if (size < candidateSize) {
            candidateShard = shardId;
            candidateSize = size;
        }
    }

    ShardScheme newScheme(old.version() + 1);

    for (size_t shardId = 0; shardId < old.getNumShards(); ++shardId) {
        ShardInfo newShard = old.getShardInfo(shardId);

        if (shardId == candidateShard)
            newShard.addNode(address);

        newScheme.addShard(newShard);
    }

    return newScheme;
}

ShardScheme
delNodeFromScheme(const ShardScheme &old, const std::string &address)
{
    // Removes address and possibly redistributes nodes.

    optional<size_t> addressShardId = old.getShardIdForAddress(address);

    // If the address was not found, don't change anything.
    if (!addressShardId)
        return old;

    // If there is only one shard, just remove the address.
    if (old.getNumShards() == 1) {
        assert(addressShardId.value() == 0);

        ShardScheme newScheme(old.version() + 1);

        ShardInfo newShard = old.getShardInfo(0);
        newShard.removeNode(address);

        newScheme.addShard(newShard);

        return newScheme;
    }

    // Otherwise, try to redistribute nodes.

    size_t newShardSize = old.getShardInfo(addressShardId.value()).getNumNodes() - 1;

    size_t largestOtherId = addressShardId.value() == 0 ? 1 : 0;
    size_t largestOtherSize = old.getShardInfo(largestOtherId).getNumNodes();
    for (size_t shardId = 1; shardId < old.getNumShards(); ++shardId) {

        // We're trying to find the largest shard other than the shard from which we
        // are removing an address.
        if (shardId == addressShardId.value())
            continue;

        size_t size = old.getShardInfo(shardId).getNumNodes();

        if (size > largestOtherSize) {
            largestOtherId = shardId;
            largestOtherSize = size;
        }
    }

    ShardScheme newScheme(old.version() + 1);

    vector<ShardInfo> newShards;

    for (size_t shardId = 0; shardId < old.getNumShards(); ++shardId)
        newShards.push_back(old.getShardInfo(shardId));

    ShardInfo &changedShard = newShards[addressShardId.value()];
    changedShard.removeNode(address);

    // Always move one node from another shard to this shard. This is a simple (but not very
    // efficient) way to keep everything balanced.
    if (largestOtherSize > 0) {
        ShardInfo &otherShard = newShards[largestOtherId];

        string movedNode = *otherShard.getNodeSet().begin();

        otherShard.removeNode(movedNode);
        changedShard.addNode(movedNode);
    }

    return newScheme;
}

} // namespace ShardSchemeUtility
