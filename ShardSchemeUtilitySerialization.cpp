#include "ParsingHelpers.h"
#include "ShardSchemeUtility.h"

#include <cstdlib>
#include <cstring>
#include <sstream>

using namespace std;

namespace
{

void
skipWhitespace(string_view &str)
{
    while (str.size() > 0 && str[0] == ' ')
        str.remove_prefix(1);
}

int
svToInt(string_view &str)
{
    const char *data = str.data();
    char *next;
    int val = strtol(data, &next, 10);

    int numCharsRead = (int)(next - data);

    str.remove_prefix(numCharsRead);

    return val;
}

size_t
svToUl(string_view &str)
{
    const char *data = str.data();
    char *next;
    size_t val = strtoul(data, &next, 10);

    int numCharsRead = (int)(next - data);

    str.remove_prefix(numCharsRead);

    return val;
}

} // namespace

namespace ShardSchemeUtility
{

string
serializeScheme(const ShardScheme &scheme, string avoidChars)
{
    stringstream builder;

    builder << scheme.version();
    builder << " ";
    builder << scheme.getNumShards();
    builder << " ";
    for (size_t id = 0; id < scheme.getNumShards(); ++id) {
        const ShardInfo &shard = scheme.getShardInfo(id);
        const auto &nodeSet = shard.getNodeSet();

        builder << shard.getHash();
        builder << " ";
        builder << nodeSet.size();
        builder << " ";

        for (const string &node : nodeSet) {
            builder << escapeChars(node, " ");
            builder << " ";
        }
    }

    string serialized = builder.str();

    // Remove trailing space.
    serialized = serialized.substr(0, serialized.size() - 1);

    return escapeChars(serialized, avoidChars);
}

ShardScheme
deserializeScheme(string_view schemeString, string avoidChars)
{
    int version = svToInt(schemeString);
    size_t numShards = svToUl(schemeString);

    ShardScheme scheme(version);

    for (int id = 0; id < numShards; ++id) {
        size_t hash = svToUl(schemeString);
        size_t numNodes = svToUl(schemeString);
        skipWhitespace(schemeString);

        ShardInfo shard(hash);

        for (size_t nodeIdx = 0; nodeIdx < numNodes; ++nodeIdx) {
            size_t nextPos = findNextUnescapedChar(schemeString, ' ');

            string address = unescapeChars(schemeString.substr(0, nextPos));
            shard.addNode(address);

            schemeString.remove_prefix(nextPos);
            skipWhitespace(schemeString);
        }

        scheme.addShard(shard);
    }

    return scheme;
}

} // namespace ShardSchemeUtility
