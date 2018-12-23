#include "ShardScheme.h"

#include "TransformIterator.h"

#include <algorithm>
#include <cassert>
#include <functional>

using namespace std;

ShardScheme::ShardScheme(int version)
    : mVersion(version)
    , mNumNodes(0)
{
}

void
ShardScheme::addShard(const ShardInfo &shard)
{
    auto itr = firstShardAboveHash(shard.getHash());

    // Insert new element. Shards will remain ordered by hash in ascending order.
    mShards.emplace(itr, shard);

    mNumNodes += shard.getNodeSet().size();
}

int
ShardScheme::version() const
{
    return mVersion;
}

size_t
ShardScheme::getNumShards() const
{
    return mShards.size();
}

size_t
ShardScheme::getNumNodes() const
{
    return mNumNodes;
}

const ShardInfo &
ShardScheme::getShardInfo(size_t idx) const
{
    return mShards[idx];
}

optional<size_t>
ShardScheme::getShardIdForAddress(const string &address) const
{
    for (size_t idx = 0; idx < mShards.size(); ++idx) {
        if (mShards[idx].getNodeSet().count(address) > 0)
            return idx;
    }

    return {};
}

const ShardInfo &
ShardScheme::getShardInfoForAddress(const string &address) const
{
    optional<size_t> idx = getShardIdForAddress(address);

    assert(idx.has_value());

    return mShards[idx.value()];
}

size_t
ShardScheme::getResponsibleShardId(size_t keyHash) const
{
    auto pos = firstShardAboveHash(keyHash);

    if (pos == mShards.end())
        return 0;
    else
        return distance(mShards.begin(), pos);
}

const ShardInfo &
ShardScheme::getResponsibleShardInfo(size_t keyHash) const
{
    return mShards[getResponsibleShardId(keyHash)];
}

vector<ShardInfo>::const_iterator
ShardScheme::firstShardAboveHash(size_t hsh) const
{
    using HashItr = TransformIterator<vector<ShardInfo>::const_iterator, size_t>;

    auto pos = upper_bound(HashItr(mShards.begin(), &ShardInfo::getHash),
                           HashItr(mShards.end(), &ShardInfo::getHash), hsh);

    return pos.underlyingIterator();
}
