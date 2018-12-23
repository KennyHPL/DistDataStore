#include "Node.h"
#include "ParseServer.h"
#include "ParsingHelpers.h"
#include "ShardSchemeUtility.h"
#include "VectorClock.h"
#include "View.h"

#include <cstdlib>
#include <iostream>
#include <pistache/async.h>
#include <pistache/endpoint.h>
#include <thread>

using namespace std;

/// Gets the view from the VIEW environment variable.
std::vector<std::string>
getView()
{
    char *viewCString = getenv("VIEW");
    if (!viewCString)
        return {};

    std::string viewString(viewCString);

    std::vector<std::string> view;

    // Split by commas.
    size_t ipStart = 0;
    size_t nextComma = viewString.find(',');

    while (nextComma != std::string::npos) {
        string sstr = viewString.substr(ipStart, nextComma - ipStart);
        view.push_back(move(sstr));

        ipStart = nextComma + 1;
        nextComma = viewString.find(',', ipStart);
    }

    if (nextComma > ipStart)
        view.push_back(viewString.substr(ipStart));

    return view;
}

/// Gets the IP and port from the IP_PORT environment variable, if any.
/// If IP_PORT is not set, this will return an empty string.
std::string
getMyAddress()
{
    char *ipPort = getenv("IP_PORT");

    if (ipPort)
        return ipPort;
    else
        return "";
}

size_t
getNumShards()
{
    char *numShardsStr = getenv("S");

    if (numShardsStr) {
        size_t numShards = strtoul(numShardsStr, nullptr, 10);

        if (numShards < 1)
            numShards = 1;

        return numShards;
    } else {
        return 1;
    }
}

int
main()
{
    string myAddr = getMyAddress();
    vector<string> allAddresses = getView();

    shared_ptr<View> view = make_shared<View>(
        myAddr, ShardSchemeUtility::createInitialShardScheme(getNumShards(), allAddresses));

    std::shared_ptr<Node> node = make_shared<Node>(view);

    std::unique_ptr<ParseServer> server = std::make_unique<ParseServer>(node);

    // Always listen on port 8080. The address from getMyAddress() is external.
    Pistache::Http::Endpoint endpoint("*:8080");
    endpoint.init(Pistache::Http::Endpoint::options().threads(10));
    endpoint.setHandler(server->handler());
    endpoint.serve();

    return 0;
}
