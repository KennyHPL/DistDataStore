#include "KvsServerNode.h"

#include <set>
#include <sstream>
#include <utility>

#define WITH_CLOCK(item) make_pair(item, mNode->getNodeClock())

using namespace std;

KvsServerNode::KvsServerNode(const string &IP, const vector<string> &viewIPs)
    : mNode(new Node(IP, viewIPs))
    , mDataStore(mNode)
{
}

pair<AbstractKvsServerNode::PutSuccessType, VectorClock>
KvsServerNode::putElement(const string &key, const string &value,
                          const VectorClock &payload)
{
    pair<string, VectorClock> p(key, payload);
    auto insertResult = mDataStore.insert(p, value);
    switch (insertResult) {
    case DataStore::InsertResult::CreatedNewValue:
        return make_pair(AbstractKvsServerNode::PutSuccessType::CreatedNewValue,
                         mNode->getNodeClock());
    case DataStore::InsertResult::UpdatedExistingValue:
        return make_pair(
            AbstractKvsServerNode::PutSuccessType::UpdatedExistingValue,
            mNode->getNodeClock());
    default:
        return make_pair(AbstractKvsServerNode::PutSuccessType::KeyNotValid,
                         mNode->getNodeClock());
    }
}

pair<optional<string>, VectorClock>
KvsServerNode::getElement(const string &key, const VectorClock &payload)
{
    return WITH_CLOCK(mDataStore.get(make_pair(key, payload)));
}

pair<bool, VectorClock>
KvsServerNode::hasElement(const string &key, const VectorClock &payload)
{
    return WITH_CLOCK(mDataStore.contains(make_pair(key, payload)));
}

pair<bool, VectorClock>
KvsServerNode::delElement(const string &key, const VectorClock &payload)
{
    return WITH_CLOCK(mDataStore.remove(make_pair(key, payload)));
}

optional<pair<string, VectorClock>>
KvsServerNode::directGet(const string &key)
{
    return mDataStore.directGet(key);
}

bool KvsServerNode::trySync()
{
    return mDataStore.trySync();
}

bool KvsServerNode::syncData(const string &data)
{
    return mDataStore.syncData(data);
}

const vector<string> &
KvsServerNode::getView() const
{
    return mNode->getView();
}

bool
KvsServerNode::addView(const string &ipPort)
{
    if (mNode->addToView(ipPort)) {
        // Propagate!
        addViewInterserver(ipPort, {});
        return true;
    } else {
        return false;
    }
}

void
KvsServerNode::addViewInterserver(const string &ipPort,
                                  const std::vector<std::string> &visited)
{
    mNode->addToView(ipPort);

    set<string> visitedSet(visited.begin(), visited.end());

    const vector<string> &view = mNode->getOtherView();
    set<string> viewSet(view.begin(), view.end());

    set<string> combined = visitedSet;
    combined.insert(viewSet.begin(), viewSet.end());
    combined.insert(mNode->getNodeIp());

    ostringstream newVisitedBuilder;
    for (const string &ip : combined)
        newVisitedBuilder << ip << ",";
    string newVisited = newVisitedBuilder.str();

    // Get rid of the trailing comma
    newVisited = newVisited.substr(0, newVisited.size() - 1);

    ostringstream newMessageBuilder;
    newMessageBuilder << "ip_port=";
    newMessageBuilder << ipPort;
    newMessageBuilder << "&&";
    newMessageBuilder << "visited=";
    newMessageBuilder << newVisited;

    string newMessage = newMessageBuilder.str();

    for (const string &ip : viewSet)
        if (visitedSet.find(ip) == visitedSet.end())
            mNode->sendMsg(ip, "view-add", newMessage);
}

bool
KvsServerNode::delView(const string &ipPort)
{
    if (mNode->removeFromView(ipPort)) {
        // Propagate!
        delViewInterserver(ipPort, {});
        return true;
    } else {
        return false;
    }
}

void
KvsServerNode::delViewInterserver(const string &ipPort,
                                  const std::vector<std::string> &visited)
{
    mNode->removeFromView(ipPort);

    set<string> visitedSet(visited.begin(), visited.end());

    const vector<string> &view = mNode->getOtherView();
    set<string> viewSet(view.begin(), view.end());

    set<string> combined = visitedSet;
    combined.insert(viewSet.begin(), viewSet.end());
    combined.insert(mNode->getNodeIp());

    ostringstream newVisitedBuilder;
    for (const string &ip : combined)
        newVisitedBuilder << ip << ",";
    string newVisited = newVisitedBuilder.str();

    // Get rid of the trailing comma
    newVisited = newVisited.substr(0, newVisited.size() - 1);

    ostringstream newMessageBuilder;
    newMessageBuilder << "ip_port=";
    newMessageBuilder << ipPort;
    newMessageBuilder << "&&";
    newMessageBuilder << "visited=";
    newMessageBuilder << newVisited;

    string newMessage = newMessageBuilder.str();

    for (const string &ip : viewSet) {
        if (visitedSet.find(ip) == visitedSet.end())
            mNode->sendMsg(ip, "view-delete", newMessage);
    }
}
