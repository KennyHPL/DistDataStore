#include "ParseServer.h"

#include "ParsingHelpers.h"
#include "ShardSchemeUtility.h"

#include <pistache/async.h>
#include <pistache/http_headers.h>
#include <set>
#include <sstream>

using namespace Pistache;
using namespace std;

ParseServer::ParseServer(shared_ptr<Node> node)
    : mNode(node)
{
    mRouter = make_shared<HttpRouter>();

    using namespace Pistache::Rest;

#define MAKE_ROUTE(method, resource, callback)                                                     \
    Routes::method(*mRouter, resource, Routes::bind(&ParseServer::callback, this))

    MAKE_ROUTE(Get, "/keyValue-store/:key", getElementImpl);
    MAKE_ROUTE(Get, "/keyValue-store/search/:key", hasElementImpl);
    MAKE_ROUTE(Put, "/keyValue-store/:key", putElementImpl);
    MAKE_ROUTE(Delete, "/keyValue-store/:key", delElementImpl);

    MAKE_ROUTE(Get, "/view", getViewImpl);
    MAKE_ROUTE(Put, "/view", addViewImpl);
    MAKE_ROUTE(Delete, "/view", delViewImpl);

    MAKE_ROUTE(Get, "/shard/my_id", getShardMyIdImpl);
    MAKE_ROUTE(Get, "/shard/all_ids", getShardAllIdsImpl);
    MAKE_ROUTE(Get, "/shard/members/:shardId", getShardMembersImpl);
    MAKE_ROUTE(Get, "/shard/count/:shardId", getShardCountImpl);
    MAKE_ROUTE(Put, "/shard/changeShardNumber", putShardChangeNumberImpl);

    MAKE_ROUTE(Patch, "/inter_server/dataStore/:key", patchInterImpl);
    MAKE_ROUTE(Patch, "/inter_server/dataSync/push", patchSyncPush);
    MAKE_ROUTE(Patch, "/inter_server/shards/prepare", shardPrepareImpl);
    MAKE_ROUTE(Patch, "/inter_server/shards/switch", shardSwitchImpl);
    MAKE_ROUTE(Patch, "/inter_server/shards/move", shardMoveImpl);

#undef MAKE_ROUTE
}

shared_ptr<Pistache::Http::Handler>
ParseServer::handler() const
{
    return mRouter->handler();
}

#define CHECK_FORWARD(KEY)                                                                         \
    string _dest = mNode->keyToNode(KEY);                                                          \
    if (!_dest.empty()) {                                                                          \
        forwardRequest(_dest, request, response);                                                  \
        return;                                                                                    \
    }

void
ParseServer::putElementImpl(const RestRequest &request, HttpResponse response)
{
PUT_ELEMENT_TOP:
    string key = request.param(":key").as<string>();
    CHECK_FORWARD(key)
    string value = getParam(request, "val");
    VectorClock payload = VectorClock::fromString(getParam(request, "payload"));

    auto putResult = mNode->putElement(key, value, payload);

    if (putResult.isBadRequest()) {
        response.send(Http::Code::Bad_Request);
        return;
    }

    if (putResult.hasWrongSchemeVersion()) {
        mNode->waitForNewSchemeVersion(putResult.newSchemeVersion);
        goto PUT_ELEMENT_TOP;
    }

    switch (putResult.value) {
    case Node::PutSuccessType::CreatedNewValue: {
        ostringstream stream;
        stream << "{" << endl;
        stream << "\"replaced\":false," << endl;
        stream << "\"msg\":\"Added successfully\"," << endl;
        stream << "\"payload\":\"" << putResult.clock.toString() << "\"" << endl;
        stream << "}" << endl;

        response.send(Http::Code::Ok, // NOTE: hw specs say this should return Ok
                      stream.str(), MIME(Application, Json));
    } break;

    case Node::PutSuccessType::UpdatedExistingValue: {
        ostringstream stream;
        stream << "{" << endl;
        stream << "\"replaced\":true," << endl;
        stream << "\"msg\":\"Updated successfully\"," << endl;
        stream << "\"payload\":\"" << putResult.clock.toString() << "\"" << endl;
        stream << "}" << endl;

        response.send(Http::Code::Created, // NOTE: hw specs say this should return Created
                      stream.str(), MIME(Application, Json));
    } break;

    default:
        response.send(Http::Code::Bad_Request);
        break;
    }
}

void
ParseServer::getElementImpl(const RestRequest &request, HttpResponse response)
{
GET_ELEM_TOP:
    string key = request.param(":key").as<string>();
    CHECK_FORWARD(key)
    VectorClock requestPayload = VectorClock::fromString(getParam(request, "payload"));
    auto getResult = mNode->getElement(key, requestPayload);

    if (getResult.isBadRequest()) {
        response.send(Http::Code::Bad_Request);
        return;
    }

    if (getResult.hasWrongSchemeVersion()) {
        mNode->waitForNewSchemeVersion(getResult.newSchemeVersion);
        goto GET_ELEM_TOP;
    }

    auto value = getResult.value;
    VectorClock payload = getResult.clock;
    auto payload_str = payload.toString();

    string owner = to_string(mNode->getView()->scheme().getResponsibleShardId(hash<string>{}(key)));

    if (value) {
        ostringstream stream;
        stream << "{" << endl;
        stream << "\"result\":\"Success\"," << endl;
        stream << "\"value\":\"" << (*value) << "\"," << endl;
        stream << "\"owner\":\"" << owner << "\"," << endl;
        stream << "\"payload\":\"" << payload.toString() << "\"" << endl;
        stream << "}" << endl;

        response.send(Http::Code::Ok, stream.str(), MIME(Application, Json));
    } else {
        ostringstream stream;
        stream << "{" << endl;
        stream << "\"result\":\"Error\"," << endl;
        stream << "\"msg\":\"Key does not exist\"," << endl;
        stream << "\"payload\":\"" << payload.toString() << "\"" << endl;
        stream << "}" << endl;

        response.send(Http::Code::Not_Found, stream.str(), MIME(Application, Json));
    }
}

void
ParseServer::hasElementImpl(const RestRequest &request, HttpResponse response)
{
HAS_ELEM_TOP:
    string key = request.param(":key").as<string>();
    CHECK_FORWARD(key)
    VectorClock requestPayload = VectorClock::fromString(getParam(request, "payload"));
    auto hasResult = mNode->hasElement(key, requestPayload);

    if (hasResult.isBadRequest()) {
        response.send(Http::Code::Bad_Request);
        return;
    }

    if (hasResult.hasWrongSchemeVersion()) {
        mNode->waitForNewSchemeVersion(hasResult.newSchemeVersion);
        goto HAS_ELEM_TOP;
    }

    bool found = hasResult.value;
    VectorClock payload = hasResult.clock;

    ostringstream stream;
    stream << "{" << endl;
    stream << "\"isExists\":" << (found ? "true" : "false") << "," << endl;
    stream << "\"result\":\"Success\"," << endl;
    stream << "\"payload\":\"" << payload.toString() << "\"" << endl;
    stream << "}" << endl;

    response.send(Http::Code::Ok, stream.str(), MIME(Application, Json));
}

void
ParseServer::delElementImpl(const RestRequest &request, HttpResponse response)
{
DEL_ELEM_TOP:
    string key = request.param(":key").as<string>();
    CHECK_FORWARD(key)
    VectorClock requestPayload = VectorClock::fromString(getParam(request, "payload"));
    auto delResult = mNode->delElement(key, requestPayload);

    if (delResult.isBadRequest()) {
        response.send(Http::Code::Bad_Request);
        return;
    }

    if (delResult.hasWrongSchemeVersion()) {
        mNode->waitForNewSchemeVersion(delResult.newSchemeVersion);
        goto DEL_ELEM_TOP;
    }

    bool deleted = delResult.value;
    VectorClock payload = delResult.clock;

    ostringstream stream;
    stream << "{" << endl;
    stream << "\"result\":" << (deleted ? "\"Success\"" : "\"Error\"") << "," << endl;
    stream << "\"msg\":" << (deleted ? "\"Key deleted\"" : "\"Key does not exist\"") << "," << endl;
    stream << "\"payload\":\"" << payload.toString() << "\"" << endl;
    stream << "}" << endl;

    response.send(deleted ? Http::Code::Ok : Http::Code::Not_Found, stream.str(),
                  MIME(Application, Json));
}

#undef CHECK_FORWARD

void
ParseServer::getViewImpl(const RestRequest &request, HttpResponse response)
{
    set<string> view = mNode->getView()->getAllAddresses();

    ostringstream stream;
    stream << "{" << endl;
    stream << "\"view\":";
    stream << "\"";

    auto it = view.begin();
    stream << *it;
    for (++it; it != view.end(); ++it) {
        stream << "," << *it;
    }

    stream << "\"" << endl;
    stream << "}" << endl;

    response.send(Http::Code::Ok, stream.str(), MIME(Application, Json));
}

void
ParseServer::addViewImpl(const RestRequest &request, HttpResponse response)
{
    string ip_port = getParam(request, "ip_port");

    bool success = mNode->addNode(ip_port);

    ostringstream stream;
    stream << "{" << endl;
    stream << "\"result\":" << (success ? "\"Success\"" : "\"Error\"") << "," << endl;
    stream << "\"msg\":" << endl;
    if (success)
        stream << "\"Successfully added " << ip_port << " to view\"" << endl;
    else
        stream << "\"" << ip_port << " is already in view\"" << endl;
    stream << "}" << endl;

    response.send(success ? Http::Code::Ok : Http::Code::Not_Found, stream.str(),
                  MIME(Application, Json));
}

void
ParseServer::delViewImpl(const RestRequest &request, HttpResponse response)
{
    string ip_port = getParam(request, "ip_port");

    bool success = mNode->delNode(ip_port);

    ostringstream stream;
    stream << "{" << endl;
    stream << "\"result\":" << (success ? "\"Success\"" : "\"Error\"") << "," << endl;
    stream << "\"msg\":";
    if (success)
        stream << "\"Successfully removed " << ip_port << " from view\"" << endl;
    else
        stream << "\"" << ip_port << " is not in current view\"" << endl;
    stream << "}" << endl;

    response.send(success ? Http::Code::Ok : Http::Code::Not_Found, stream.str(),
                  MIME(Application, Json));
}

void
ParseServer::patchInterImpl(const RestRequest &request, HttpResponse response)
{
    string key = request.param(":key").as<string>();

    auto val = mNode->directGet(key);
    if (!val.has_value())
        response.send(Http::Code::Ok, "", MIME(Application, Json));
    else
        response.send(Http::Code::Ok, dataVersionAndSchemeVersionToString(*val),
                      MIME(Application, Json));
}

void
ParseServer::patchSyncPush(const RestRequest &request, HttpResponse response)
{
    bool result = mNode->syncData(request.body());
    response.send(result ? Http::Code::Ok : Http::Code::Not_Found, "", MIME(Application, Json));
}

void
ParseServer::forwardRequest(const string &dest, const RestRequest &request, HttpResponse &response)
{
    auto requestBuilder = mNode->requestBuilder();
    requestBuilder.method(request.method())
        .resource(dest + request.resource())
        .params(request.query())
        .body(request.body());

    auto _response = requestBuilder.send();
    _response.then(
        [&response](Http::Response rsp) {
            response.send(rsp.code(), rsp.body(), MIME(Application, Json));
        },
        Async::IgnoreException);

    Async::Barrier barrier(_response);
    barrier.wait();
}

void
ParseServer::shardPrepareImpl(const RestRequest &request, HttpResponse response)
{
    string requestBody = request.body();
    ShardScheme scheme = ShardSchemeUtility::deserializeScheme(requestBody);

    bool success = mNode->reshardPrepare(scheme);

    if (success)
        response.send(Http::Code::Ok);
    else
        response.send(Http::Code::Bad_Request);
}

void
ParseServer::shardSwitchImpl(const RestRequest &request, HttpResponse response)
{
    int version = stoi(request.body());

    bool success = mNode->reshardSwitch(version);

    if (success)
        response.send(Http::Code::Ok);
    else
        response.send(Http::Code::Bad_Request);
}

void
ParseServer::getShardMyIdImpl(const RestRequest &request, HttpResponse response)
{
    auto myShard = mNode->getView()->getShardId();

    ostringstream stream;
    stream << "{" << endl;
    stream << "\"id\":"
           << "\"" << myShard.value_or(-1) << "\"" << endl;
    stream << "}" << endl;

    response.send(Http::Code::Ok, stream.str(), MIME(Application, Json));
}

void
ParseServer::getShardAllIdsImpl(const RestRequest &request, HttpResponse response)
{
    size_t numShards = mNode->getView()->scheme().getNumShards();

    ostringstream stream;
    stream << "{" << endl;
    stream << "\"result\":\"Success\"," << endl;
    stream << "\"shard_ids\": \"0";
    for (int i = 1; i < numShards; ++i)
        stream << "," << i;
    stream << "\"" << endl;
    stream << "}" << endl;

    response.send(Http::Code::Ok, stream.str(), MIME(Application, Json));
}

void
ParseServer::getShardMembersImpl(const RestRequest &request, HttpResponse response)
{
    int shardId = request.param(":shardId").as<int>();
    bool success = shardId >= 0 && shardId < mNode->getView()->scheme().getNumShards();
    if (success) {
        const set<string> &shardMembers =
            mNode->getView()->scheme().getShardInfo(shardId).getNodeSet();

        auto it = shardMembers.cbegin();

        ostringstream stream;
        stream << "{" << endl;
        stream << "\"result\":\"Success\"," << endl;
        stream << "\"members\":\"" << *it;
        while (++it != shardMembers.cend())
            stream << "," << *it;
        stream << "\"" << endl;
        stream << "}" << endl;

        response.send(Http::Code::Ok, stream.str(), MIME(Application, Json));
    } else {
        ostringstream stream;
        stream << "{" << endl;
        stream << "\"result\":\"Error\"," << endl;
        stream << "\"msg\":\"No shard with id " << shardId << "\"" << endl;

        response.send(Http::Code::Not_Found, stream.str(), MIME(Application, Json));
    }
}

void
ParseServer::getShardCountImpl(const RestRequest &request, HttpResponse response)
{
    int shardId = request.param(":shardId").as<int>();
    bool success = shardId >= 0 && shardId < mNode->getView()->scheme().getNumShards();
    if (success) {
        size_t count = 0;

        if (mNode->getView()->getShardId() == shardId) {
            count = mNode->count();
        } else {
            const set<string> &shardMembers =
                mNode->getView()->scheme().getShardInfo(shardId).getNodeSet();

            mNode->sendToRandomNodeUntilSuccess(shardMembers, "/count", "",
                                                [&count](Pistache::Http::Response r) -> bool {
                                                    count = atoi(r.body().c_str());
                                                    return true;
                                                });
        }

        ostringstream stream;
        stream << "{" << endl;
        stream << "\"result\":\"Success\"," << endl;
        stream << "\"Count\":\"" << count << "\"" << endl;
        stream << "}" << endl;

        response.send(Http::Code::Ok, stream.str(), MIME(Application, Json));
    } else {
        ostringstream stream;
        stream << "{" << endl;
        stream << "\"result\":\"Error\"," << endl;
        stream << "\"msg\":\"No shard with id " << shardId << "\"" << endl;

        response.send(Http::Code::Not_Found, stream.str(), MIME(Application, Json));
    }
}

void
ParseServer::putShardChangeNumberImpl(const RestRequest &request, HttpResponse response)
{
    size_t numShards = atoi(getParam(request, "num").c_str());
    bool result = mNode->reshard(numShards);

    if (result) {
        ostringstream stream;
        stream << "{" << endl;
        stream << "\"result\":\"Success\"," << endl;
        stream << "\"shard_ids\": \"0";
        for (int i = 1; i < numShards; ++i)
            stream << "," << i;
        stream << "\"" << endl;
        stream << "}" << endl;

        response.send(Http::Code::Ok, stream.str(), MIME(Application, Json));
    } else {
        ostringstream stream;
        stream << "{" << endl;
        stream << "\"result\":\"Error\"," << endl;
        stream << "\"msg\":\"Not enough nodes";

        size_t numNodes = mNode->getView()->scheme().getNumNodes();

        if (numNodes >= numShards)
            stream << ". " << numShards << " shards results in a nunfault tolerant shard\"";
        else
            stream << " for " << numShards << " shards\"";

        stream << endl;
        stream << "}" << endl;

        response.send(Http::Code::Bad_Request, stream.str(), MIME(Application, Json));
    }
}

void
ParseServer::shardMoveImpl(const RestRequest &request, HttpResponse response)
{
    const string &body = request.body();

    size_t firstAmpersand, secondAmpersand;

    // Find the first and second unescaped ampersands.
    {
        string requestBody = body;
        string_view bodyRemaining = requestBody;

        firstAmpersand = findNextUnescapedChar(bodyRemaining, '&');

        if (firstAmpersand >= bodyRemaining.size()) {
            response.send(Http::Code::Bad_Request);
            return;
        }

        bodyRemaining.remove_prefix(firstAmpersand);
        secondAmpersand = findNextUnescapedChar(bodyRemaining, '&');

        if (secondAmpersand >= bodyRemaining.size()) {
            response.send(Http::Code::Bad_Request);
            return;
        }
    }

    string versionStr = body.substr(0, firstAmpersand);
    string keyStr = body.substr(firstAmpersand + 1, secondAmpersand - firstAmpersand - 1);
    string valStr = body.substr(secondAmpersand + 1, body.size() - secondAmpersand - 1);

    int version = stoi(versionStr);
    Node::DataVersion dataVersion = stringToDataVersion(valStr);

    bool success = mNode->reshardMove(version, keyStr, dataVersion);

    if (success)
        response.send(Http::Code::Ok);
    else
        response.send(Http::Code::Payment_Required);
}
