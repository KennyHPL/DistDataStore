#pragma once

#include <optional>
#include <set>
#include <string>
#include <vector>

class ShardInfo;
class ShardScheme;

/// NOTE: Shard IDs are indices.
class ShardScheme
{
public:
    /// Creates an empty shard scheme.
    ShardScheme(int version = 0);

    /// Add a shard. Not thread-safe.
    void addShard(const ShardInfo &shard);

    /// Returns the scheme version.
    int version() const;

    /// Returns the total number of shards in the scheme. Shard IDs are indices, so this
    /// can be used to enumerate all shards.
    size_t getNumShards() const;

    /// Returns the total number of nodes in the scheme.
    size_t getNumNodes() const;

    /// Returns the info for a particular shard. This contains the list of
    /// nodes in the shard and also its hash.
    const ShardInfo &getShardInfo(size_t shardId) const;

    /// Returns the ID of the first shard containing this address.
    std::optional<size_t> getShardIdForAddress(const std::string &addr) const;

    /// Helper function equivalent to getShardInfo(getShardIdForAddress(addr)).
    /// Assumes that the optional returned by getShardIdForAddress() is nonempty.
    const ShardInfo &getShardInfoForAddress(const std::string &addr) const;

    /// Returns the ID of the shard responsible for the key hash. Assumes that
    /// there is at least one shard.
    size_t getResponsibleShardId(size_t keyHash) const;

    /// Helper function equivalent to getShardInfo(getResponsibleShardId(keyHash)).
    const ShardInfo &getResponsibleShardInfo(size_t keyHash) const;

private:
    /// Finds the first shard in mShards for which getHash() > hash. Returns the
    /// iterator to its position (possibly equal to mShards.end()).
    std::vector<ShardInfo>::const_iterator firstShardAboveHash(size_t hash) const;

    std::vector<ShardInfo> mShards;
    int mVersion;
    size_t mNumNodes;
};

class ShardInfo
{
public:
    /// Creates an empty shard with a given hash.
    ShardInfo(size_t hash)
        : mHash(hash)
    {
    }

    /// Adds a new node to the shard.
    void addNode(const std::string &node) { mNodes.insert(node); }

    /// Removes a node from the shard.
    void removeNode(const std::string &node) { mNodes.erase(node); }

    /// Returns the shard's hash.
    size_t getHash() const { return mHash; }

    size_t getNumNodes() const { return mNodes.size(); }

    /// Returns the nodes inside the shard.
    const std::set<std::string> &getNodeSet() const { return mNodes; }

private:
    std::set<std::string> mNodes;
    size_t mHash;
};
