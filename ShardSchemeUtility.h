#pragma once

#include "ShardScheme.h"

#include <string>
#include <vector>

namespace ShardSchemeUtility
{

/// Creates an initial shard scheme. This function is completely deterministic.
ShardScheme createInitialShardScheme(size_t numShards, std::vector<std::string> allAddresses);

/// Creates a new shard scheme so that switching from the old scheme to
/// the new scheme requires moving only a small amount of data.
ShardScheme createNewShardScheme(const ShardScheme &old, size_t numShards);

/// Returns a scheme with the new address added to a shard in the most
/// efficient manner.
ShardScheme addNodeToScheme(const ShardScheme &old, const std::string &address);

/// Returns a scheme with the given node removed. Ensures that nodes
/// are spread uniformly across shards (so if there are enough nodes,
/// every shard will have at least 1 or at least 2).
ShardScheme delNodeFromScheme(const ShardScheme &old, const std::string &address);

/// Writes the scheme to a string, escaping characters in avoidChars with backslashes.
/// E.g. if the scheme would be printed to the string "my-scheme" and avoidChars is just "-",
/// the output will be "my\-scheme".
std::string serializeScheme(const ShardScheme &scheme, std::string avoidChars = "");

/// Reads the scheme from the string, assuming it was written with serializeScheme with the
/// same avoidChars. Specifically, the sequence of operations
/// `deserializeScheme(serializeScheme(scheme, avoidChars), avoidChars)` returns
/// a copy of `scheme`.
ShardScheme
deserializeScheme(std::string_view schemeString, std::string avoidedChars = "");

} // namespace ShardSchemeUtility
