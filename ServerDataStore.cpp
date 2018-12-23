#include "ServerDataStore.h"

#include "ParsingHelpers.h"

#include <chrono>
#include <optional>
#include <pistache/async.h>
#include <vector>

using namespace std;
using namespace Pistache;

DataStore::InsertResult
ServerDataStore::insert(const pair<string, VectorClock> &key, const string &value)
{
    lock_guard<mutex> lk(mServerMut);
    mNode->nodeClockMerge(key.second).nodeClockIncrement();

    auto ret =
        mLocalDataStore.insert(key.first, pair<string, VectorClock>(value, mNode->getNodeClock()));

    dataStateChanged();

    return ret;
}

bool
ServerDataStore::remove(const pair<string, VectorClock> &key)
{
    lock_guard<mutex> lk(mServerMut);
    mNode->nodeClockMerge(key.second).nodeClockIncrement();
    auto val = mLocalDataStore.get(key.first);

    if (!val.has_value() || val->first.empty())
        return false;

    mLocalDataStore.insert(key.first, pair<string, VectorClock>("", mNode->getNodeClock()));

    dataStateChanged();

    return true;
}

void
ServerDataStore::clear()
{
    // Not implemented. Could just "remove" all local values, but what about
    // values not on this node?
}

void
ServerDataStore::addNode(string index)
{
    for (auto it = mLocalDataStore.hashMap().begin(); it != mLocalDataStore.hashMap().end(); ++it) {
        it->second.second.addNode(index);
    }

    dataStateChanged();
}

optional<string>
ServerDataStore::get(const pair<string, VectorClock> &key)
{
    lock_guard<mutex> lk(mServerMut);
    const VectorClock &Vr = key.second;
    mNode->nodeClockMerge(Vr);

    auto localDataO = mLocalDataStore.get(key.first);

    vector<pair<string, VectorClock>> versions;

    if (localDataO.has_value()) {
        versions.push_back(*localDataO);

        VectorClock::CompareValue cv = Vr.compare(localDataO->second);

        if (cv != VectorClock::GreaterThan) {
            mNode->nodeClockIncrement();
            if (localDataO->first.empty())
                return optional<string>();
            return optional<string>(localDataO->first);
        }
    }

    // need to collect other versions
    const vector<string> &otherNodes = mNode->getOtherView();
    if (!otherNodes.empty()) {
        unordered_map<string, int> nodeIndex;
        for (int i = 0; i < otherNodes.size(); ++i)
            nodeIndex[otherNodes[i]] = i;

        string responseRawText[otherNodes.size()];

        // we only need to look at one version at a time to be optimal, but it
        // is probably faster to query them all asynchronous
        vector<Async::Promise<Http::Response>> responses;
        for (const string &oip : otherNodes) {
            auto rsp = mNode->sendMsg(oip, "dataStore/" + key.first, "");
            rsp.then(
                [&](Http::Response r) {
                    // we collect the raw bodies
                    responseRawText[nodeIndex[oip]] = r.body();
                },
                Async::IgnoreException);

            responses.push_back(move(rsp));
        }

        // TODO could start processing right away, maybe using condition
        // Variables
        auto sync = Async::whenAll(responses.begin(), responses.end());
        Async::Barrier<vector<Http::Response>> barrier(sync);
        barrier.wait();
        // Have all responces, unless some messages failed to get through

        for (int i = 0; i < otherNodes.size(); ++i) {
            // We parse the raw text to get a data-version
            if (responseRawText[i].empty()) {
                // yikes... Need to handle this. Could be that node doesn't have
                // the value, or we didn't get a response. For now we just
                // assume everything is okay...
            } else {
                versions.push_back(stringToDataVersion(responseRawText[i]));
            }
        }
    }

    if (versions.empty()) {
        mNode->nodeClockIncrement();
        return optional<string>(); // Key not found
        // TODO is this right?
    }

    pair<string, VectorClock> &max = versions[0];
    mNode->nodeClockMerge(versions[0].second);
    for (int i = 1; i < versions.size(); ++i) {
        mNode->nodeClockMerge(versions[i].second);
        if (VectorClock::isMax(versions[i].second, max.second))
            max = versions[i];
    }

    // we store this greatest value
    // TODO might be our own, so we could avoid the insert
    mLocalDataStore.insert(key.first, max);

    mNode->nodeClockIncrement();

    if (max.first.empty())
        return optional<string>();
    return optional<string>(max.first);
}

bool
ServerDataStore::contains(const pair<string, VectorClock> &key)
{
    lock_guard<mutex> lk(mServerMut);
    // TODO Should check other nodes (Not needed for this assignment)
    auto value = mLocalDataStore.get(key.first);
    return value.has_value() && !value->first.empty();
}

unsigned
ServerDataStore::count()
{
    lock_guard<mutex> lk(mServerMut);
    // TODO Should check other nodes (Not needed for this assignment)
    return mLocalDataStore.count();
}

optional<pair<string, VectorClock>>
ServerDataStore::directGet(const string &key)
{
    lock_guard<mutex> lk(mServerMut);
    return mLocalDataStore.get(key);
}

bool
ServerDataStore::syncData(const string &data)
{
    lock_guard<mutex> serverLock(mServerMut);
    if (!mIsSyncing) return false;
    lock_guard<mutex> lk(mSyncMut);
    unordered_map<string, pair<string, VectorClock>> map = dataStringToMap(data);

    for (auto it = map.begin(); it != map.end(); ++it) {
        auto myVal = mLocalDataStore.get(it->first);
        if (!myVal || !VectorClock::isMax(myVal->second, it->second.second)) {
            mLocalDataStore.insert(it->first, it->second);
        }
    }

    mIsSyncing = false;
    return true;
}

void
ServerDataStore::startSync()
{
    thread(&ServerDataStore::syncThread, this).detach();
}

void
ServerDataStore::syncThread()
{
    while (true) {
        this_thread::sleep_for(chrono::milliseconds(100));
        mSyncMut.lock();

        if (mIsSyncing && mSyncWaitCounter++ > 10) mIsSyncing = false;

        if (mNeedToSync.empty()) {
            mSyncMut.unlock();
            continue;
        }

        const string &otherIP = mNeedToSync[rand() % mNeedToSync.size()];

        bool canSync = false;
        auto rsp = mNode->sendMsg(otherIP, "dataSync/setup", "");
        rsp.then([&](Http::Response r) { canSync = r.code() == Http::Code::Ok; },
                 Async::IgnoreException);

        Async::Barrier br(rsp);
        br.wait();

        if (canSync) {
            bool syncResult = false;
            auto syncRsp = mNode->sendMsg(otherIP, "dataSync/push", mapToDataString(mLocalDataStore.hashMap()));
            syncRsp.then([&](Http::Response r) { syncResult = r.code() == Http::Code::Ok; },
                     Async::IgnoreException);

            Async::Barrier br(syncRsp);
            br.wait();

            if (syncResult)
                mNeedToSync.pop_back();
        }

        mSyncMut.unlock();
    }
}

void
ServerDataStore::dataStateChanged()
{
    mNeedToSync.clear();
    mNeedToSync = mNode->getOtherView();
}
