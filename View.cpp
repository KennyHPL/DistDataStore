#include "View.h"

using namespace std;

View::View(const string &address, const ShardScheme &shardScheme)
    : mAddress(address)
    , mShardScheme(shardScheme)
    , mShardId(mShardScheme.getShardIdForAddress(address))
{
    // clang-format off
    auto opts = Pistache::Http::Client::options()
                    .threads(1)
                    .keepAlive(true)
                    .maxConnectionsPerHost(8);
    // clang-format on

    mClient.init(opts);
}

View::~View() { mClient.shutdown(); }

const string &
View::getAddress() const
{
    return mAddress;
}

optional<size_t>
View::getShardId() const
{
    return mShardId;
}

const ShardScheme &
View::scheme() const
{
    return mShardScheme;
}

bool
View::isResponsibleFor(size_t keyHash) const
{
    if (!mShardId.has_value())
        return false;

    return mShardId.value() == mShardScheme.getResponsibleShardId(keyHash);
}

bool
View::hasAddress(const string &address) const
{
    return mShardScheme.getShardIdForAddress(address).has_value();
}

set<string>
View::getAllAddresses() const
{
    set<string> addresses;

    for (size_t id = 0; id < mShardScheme.getNumShards(); ++id) {
        const ShardInfo &shard = mShardScheme.getShardInfo(id);
        const auto &nodes = shard.getNodeSet();
        addresses.insert(nodes.begin(), nodes.end());
    }

    return addresses;
}

set<string>
View::getAddressesInShard() const
{
    if (mShardId)
        return mShardScheme.getShardInfo(mShardId.value()).getNodeSet();
    else
        return {mAddress};
}

Pistache::Async::Promise<Pistache::Http::Response>
View::sendMsg(const string &address, const string &resource, const string &msg,
              chrono::milliseconds timeout) const
{
    Pistache::Http::RequestBuilder requestBuilder = mClient.get("");

    // clang-format off
    requestBuilder
        .method(Pistache::Http::Method::Patch)
        .resource(address + "/inter_server/" + resource)
        .body(msg)
        .timeout(timeout);
    // clang-format on

    return requestBuilder.send();
}
