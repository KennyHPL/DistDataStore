#pragma once

#include "ShardScheme.h"

#include <optional>
#include <pistache/client.h>
#include <pistache/http.h>
#include <set>
#include <string>

/// A View belongs to a node in our distributed system. It contains information
/// about the node's shard, the total number of shards in the system, the
/// addresses of all other nodes in the system, and which shard each of the other
/// nodes belongs to.
class View
{
public:
    /// Creates a view with a sharding scheme.
    View(const std::string &address, const ShardScheme &shardScheme = ShardScheme());

    ~View();

    /// Returns this node's address.
    const std::string &getAddress() const;

    /// Returns this node's shard ID. This might be None if the node is not within any shard.
    std::optional<size_t> getShardId() const;

    /// Returns this view's shard scheme.
    ///
    /// @remark The short naming is for convenience.
    const ShardScheme &scheme() const;

    /// Returns true if this node is responsible for this key hash.
    bool isResponsibleFor(size_t keyHash) const;

    /// Returns whether the given address is in the view.
    bool hasAddress(const std::string &address) const;

    /// Returns all addresses in the view, including the address of this node.
    std::set<std::string> getAllAddresses() const;

    /// Returns all addresses in the same shard as this node, including the address of this node.
    std::set<std::string> getAddressesInShard() const;

    /// Sends a message to another node. The message is sent with the PATCH method
    /// to inter_server/$resource, with $msg being sent in the body.
    ///
    /// Thread-safe.
    Pistache::Async::Promise<Pistache::Http::Response>
    sendMsg(const std::string &address, const std::string &resource, const std::string &msg,
            std::chrono::milliseconds timeout = std::chrono::milliseconds::zero()) const;

    Pistache::Http::RequestBuilder requestBuilder() { return mClient.get(""); }

private:
    const std::string mAddress;
    const ShardScheme mShardScheme;
    const std::optional<size_t> mShardId;

    mutable Pistache::Http::Client mClient;
};
