#include "Node.h"
#include "VectorClock.h"

#include <memory>
#include <optional>
#include <pistache/endpoint.h>
#include <pistache/router.h>
#include <string>

class ParseServer
{
    using RestRequest = Pistache::Rest::Request;
    using HttpRouter = Pistache::Rest::Router;
    using HttpResponse = Pistache::Http::ResponseWriter;
    using string = std::string;

public:
    ParseServer(std::shared_ptr<Node> node);

    /// Gets the HTTP handler for this server. Set this as the handler
    /// of an Http::Endpoint.
    std::shared_ptr<Pistache::Http::Handler> handler() const;

private:
    // KVS:
    void putElementImpl(const RestRequest &request, HttpResponse response);

    void getElementImpl(const RestRequest &request, HttpResponse response);

    void hasElementImpl(const RestRequest &request, HttpResponse response);

    void delElementImpl(const RestRequest &request, HttpResponse response);

    // VIEW:
    void getViewImpl(const RestRequest &request, HttpResponse response);

    void addViewImpl(const RestRequest &request, HttpResponse response);

    void delViewImpl(const RestRequest &request, HttpResponse response);

    //Shard
    void getShardMyIdImpl(const RestRequest &request, HttpResponse response);
    void getShardAllIdsImpl(const RestRequest &request, HttpResponse response);
    void getShardMembersImpl(const RestRequest &request, HttpResponse response);
    void getShardCountImpl(const RestRequest &request, HttpResponse response);
    void putShardChangeNumberImpl(const RestRequest &request, HttpResponse response);

    // INTERSERVER:
    void patchInterImpl(const RestRequest &request, HttpResponse response);

    void patchSyncPush(const RestRequest &request, HttpResponse response);

    //Forwarding:
    void forwardRequest(const string &dest, const RestRequest &request, HttpResponse &response);

    // INTERSERVER SHARDS:
    void shardPrepareImpl(const RestRequest &request, HttpResponse response);

    void shardSwitchImpl(const RestRequest &request, HttpResponse response);

    void shardMoveImpl(const RestRequest &request, HttpResponse response);

    std::shared_ptr<Node> mNode;
    std::shared_ptr<HttpRouter> mRouter;
};
