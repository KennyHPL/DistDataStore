#include "Node.h"

#include "ExtraUtils.h"
#include "ParsingHelpers.h"
#include "ShardSchemeUtility.h"

#include <chrono>
#include <pistache/async.h>
#include <set>
#include <thread>
#include <vector>

// time between sync attempts in milliseconds
#define SYNC_PERIOD 150
// small time period to mix things up (maybe prevents a live lock one day?)
#define SYNC_SALT 7
#define N_RETURN(type, value) return Node::ClientOpReturnValue<type>(value, mNodeClock)

using namespace std;

namespace
{
bool
parsePrepareSuccess(Pistache::Http::Response response)
{
    if (response.code() == Pistache::Http::Code::Ok)
        return true;
    else
        return false;
}

bool
parseSwitchSuccess(Pistache::Http::Response response)
{
    if (response.code() == Pistache::Http::Code::Ok)
        return true;
    else
        return false;
}

} // namespace

Node::Node(shared_ptr<View> view)
    : mView(view)
    , mViewsReadSema(1)
    , mReshardSwitchingSema(1)
    , mPreparedView(nullptr)
    , mPreparedDatastore(nullptr)
{
    thread(&Node::syncThread, this).detach();
}

Node::ClientOpReturnValue<Node::PutSuccessType>
Node::putElement(const string &key, const string &value, const VectorClock &payload)
{
    // Prevent mView and mPreparedView from changing during this method.
    mViewsReadChangeMut.lock();
    SemaphoreDecrementGuard semaGuard(mViewsReadSema);
    mViewsReadChangeMut.unlock();

    lock_guard<mutex> lk(mClientOperationMut);

    if (key.empty())
        N_RETURN(PutSuccessType, PutSuccessType::KeyNotValid);

    lock_guard<mutex> lkd(mLocalDataMut);

    mergeAndIncrementClock(payload);

    PutSuccessType pst = PutSuccessType::CreatedNewValue;
    auto it = mLocalData.find(key);
    if (it != mLocalData.end() && !it->second.value.empty())
        pst = PutSuccessType::UpdatedExistingValue;

    insertOrReplace(mLocalData, key, DataVersion(value, mNodeClock));

    N_RETURN(PutSuccessType, pst);
}

Node::ClientOpReturnValue<optional<string>>
Node::getElement(const string &key, const VectorClock &payload)
{
    // 1000 ms timeout on interactions with other nodes
    const chrono::milliseconds TIMEOUT = 1000ms;

    // Prevent mView and mPreparedView from changing during this method.
    mViewsReadChangeMut.lock();
    SemaphoreDecrementGuard semaGuard(mViewsReadSema);
    mViewsReadChangeMut.unlock();

    lock_guard<mutex> lk(mClientOperationMut);

    mergeAndIncrementClock(payload);

    vector<DataVersion> versions;

    mLocalDataMut.lock();
    auto myIt = mLocalData.find(key);
    if (myIt != mLocalData.end()) {
        versions.push_back(myIt->second);
        mLocalDataMut.unlock();

        VectorClock::CompareValue cv = payload.compare(versions.back().clock);

        if (cv != VectorClock::GreaterThan) {
            if (versions.back().value.empty())
                N_RETURN(optional<string>, optional<string>());
            N_RETURN(optional<string>, optional<string>(versions.back().value));
        }
    } else {
        mLocalDataMut.unlock();
    }

    // collecting other versions
    // TODO right now we don't check shardScheme version. This should be fine, as direct get doesn't
    // care about version. So if we are outdated, the returned value might be a little off, but I'll
    // at least still give a valid data version (though possibly not causaly consistent)
    // To save this, maybe we assume the shard version is correct  (it would be confirmed prior to
    // calling this function)
    const set<string> &myShard = mView->getAddressesInShard();

    if (!myShard.empty()) {
        const string &me = mView->getAddress();
        unordered_map<string, int> nodeIndex;
        int i = 0;
        for (const string &s : myShard) {
            if (s == me)
                continue;
            nodeIndex[s] = i++;
        }

        if (!nodeIndex.empty()) {
            // Helps protect against segfaults.
            shared_ptr<bool> guard = make_shared<bool>();

            string responseRawText[nodeIndex.size()];

            vector<Pistache::Async::Promise<Pistache::Http::Response>> responses;
            for (auto it = nodeIndex.begin(); it != nodeIndex.end(); ++it) {

                // Protects against segfaults.
                weak_ptr<bool> guard_weakptr(guard);

                int idx = it->second;

                auto rsp = mView->sendMsg(it->first, "dataStore/" + key, "");
                rsp.then(
                    [guard_weakptr, idx, &responseRawText](Pistache::Http::Response r) {
                        // Avoid accessing invalid memory. If the guard has expired,
                        // that means we are out of scope. TODO: This is just a quickfix
                        // to the issue mentioned below at barrier.wait_for().
                        if (guard_weakptr.expired())
                            return;

                        // we collect the raw bodies
                        responseRawText[idx] = r.body();
                    },
                    Pistache::Async::IgnoreException);

                responses.push_back(move(rsp));
            }

            // TODO could start processing right away, maybe using condition
            // Variables
            auto sync = Pistache::Async::whenAll(responses.begin(), responses.end());
            Pistache::Async::Barrier<vector<Pistache::Http::Response>> barrier(sync);

            bool gotTimeout = false;
            {
                // TODO: The issue with this approach is that the message-sends that were
                // started earlier are not killed. This is a memory leak, and also if
                // code in those lambdas ever runs this could be a segfault.
                cv_status result = barrier.wait_for(TIMEOUT);
                if (result == cv_status::timeout)
                    gotTimeout = true;
            }

            // Have all responses, unless some messages failed to get through

            for (int i = 0; i < nodeIndex.size(); ++i) {
                // We parse the raw text to get a data-version
                if (!responseRawText[i].empty()) {
                    auto _ver = stringTodataVersionAndSchemeVersion(responseRawText[i]);
                    if (_ver.second > mView->scheme().version())
                        return ClientOpReturnValue<optional<string>>(_ver.second);
                    versions.push_back(_ver.first);
                }
            }

            // If we didn't find a causally consistent version of the data and at least one node
            // didn't respond, it's possible that that node has a causally consistent version.
            // Therefore we must fail out.
            if (gotTimeout)
                return {}; // Bad_Request
        }
    }

    if (versions.empty())
        N_RETURN(optional<string>, optional<string>());

    DataVersion &max = versions[0];
    mergeClock(max.clock);
    for (int i = 1; i < versions.size(); ++i) {
        mergeClock(versions[i].clock);
        if (VectorClock::isMax(versions[i].clock, max.clock))
            max = versions[i];
    }

    mLocalDataMut.lock();
    insertOrReplace(mLocalData, key, max);
    mLocalDataMut.unlock();

    if (max.value.empty())
        N_RETURN(optional<string>, optional<string>());
    N_RETURN(optional<string>, optional<string>(max.value));
}

Node::ClientOpReturnValue<bool>
Node::hasElement(const string &key, const VectorClock &payload)
{
    // Prevent mView and mPreparedView from changing during this method.
    mViewsReadChangeMut.lock();
    SemaphoreDecrementGuard semaGuard(mViewsReadSema);
    mViewsReadChangeMut.unlock();

    lock_guard<mutex> lk(mClientOperationMut);
    lock_guard<mutex> lkd(mLocalDataMut);

    // Doesn't guarantee the item isn't on another node. (But it hopes really hard)

    auto it = mLocalData.find(key);
    N_RETURN(bool, it != mLocalData.end() && !it->second.value.empty());
}

Node::ClientOpReturnValue<bool>
Node::delElement(const string &key, const VectorClock &payload)
{
    // Prevent mView and mPreparedView from changing during this method.
    mViewsReadChangeMut.lock();
    SemaphoreDecrementGuard semaGuard(mViewsReadSema);
    mViewsReadChangeMut.unlock();

    lock_guard<mutex> lk(mClientOperationMut);
    lock_guard<mutex> lkd(mLocalDataMut);

    mergeAndIncrementClock(payload);

    auto it = mLocalData.find(key);
    if (it == mLocalData.end() || it->second.value.empty())
        N_RETURN(bool, false);

    it->second = DataVersion("", mNodeClock);

    N_RETURN(bool, true);
}

size_t
Node::count()
{
    lock_guard<mutex> lk(mLocalDataMut);
    size_t cnt = 0;
    for (const pair<string, DataVersion> &d : mLocalData) {
        if (!d.second.value.empty())
            ++cnt;
    }

    return cnt;
}

bool
Node::addNode(const string &ipPort)
{
    lock_guard<mutex> lk(mClientOperationMut);

    if (mView->hasAddress(ipPort))
        return false;

    ShardScheme newScheme = ShardSchemeUtility::addNodeToScheme(mView->scheme(), ipPort);
    updateShardScheme(newScheme);

    return true;
}

bool
Node::delNode(const string &ipPort)
{
    lock_guard<mutex> lk(mClientOperationMut);

    if (!mView->hasAddress(ipPort))
        return false;

    ShardScheme newScheme = ShardSchemeUtility::delNodeFromScheme(mView->scheme(), ipPort);
    updateShardScheme(newScheme);

    return true;
}

bool
Node::reshard(size_t numShards)
{
    lock_guard<mutex> lk(mClientOperationMut);

    if (numShards * 2 > mView->scheme().getNumNodes())
        return false;

    ShardScheme newScheme = ShardSchemeUtility::createNewShardScheme(mView->scheme(), numShards);
    updateShardScheme(newScheme);

    return true;
}

optional<std::pair<Node::DataVersion, int>>
Node::directGet(const string &key)
{
    lock_guard<mutex> lk(mLocalDataMut);
    auto it = mLocalData.find(key);
    if (it != mLocalData.end())
        return make_pair(it->second, mView->scheme().version());
    return {};
}

bool
Node::syncData(const string &data)
{
    mLocalDataMut.lock();
    unordered_map<string, DataVersion> map = dataStringToMap(data);

    for (auto it = map.begin(); it != map.end(); ++it) {
        auto myVal = mLocalData.find(it->first);
        if (myVal == mLocalData.end() ||
            !VectorClock::isMax(myVal->second.clock, it->second.clock)) {
            insertOrReplace(mLocalData, it->first, it->second);
        }
    }
    mLocalDataMut.unlock();
    return true;
}

string
Node::keyToNode(const string &key) const
{
    auto myId = mView->getShardId();
    size_t keyId = mView->scheme().getResponsibleShardId(hash<string>{}(key));
    if (myId && *myId == keyId)
        return "";

    const ShardInfo &shardInfo = mView->scheme().getShardInfo(keyId);
    assert(shardInfo.getNumNodes() != 0);
    int n = rand() % shardInfo.getNumNodes();

    auto it = shardInfo.getNodeSet().cbegin();
    while (n > 0) {
        ++it;
        --n;
    }

    return *it;
}

bool
Node::reshardPrepare(const ShardScheme &newScheme)
{
    if (!mReshardSwitchingSema.tryDown())
        return false;

    SemaphoreDownGuard viewGuard(mViewsReadSema);

    mPreparedView = make_unique<View>(mView->getAddress(), newScheme);
    mPreparedDatastore = make_unique<DataStore>();

    mReshardSwitchingSema.up();

    return true;
}

bool
Node::reshardSwitch(int version)
{
    // TODO: For debugging, we may want to return a bool and a message.

    {
        SemaphoreDecrementGuard viewGuard(mViewsReadSema);

        if (mView->scheme().version() == version)
            return true;

        if (!mPreparedView || mPreparedView->scheme().version() != version)
            return false;
    }

    // If a switch is curently happening, wait for it to finish and return success. This
    // assumes that a reshardSwitch will always succeed if it lowered the semaphore.
    if (!mReshardSwitchingSema.tryDown()) {
        mReshardSwitchingSema.wait();
        return true;
    }

    {
        // Prevent mView and mPreparedView from changing during this operation.
        SemaphoreDecrementGuard viewGuard(mViewsReadSema);

        // Lock the local data for the for-loop.
        lock_guard<mutex> dataLock(mLocalDataMut);

        // Semaphores for data that are being moved.
        vector<SemaphorePtr> moveSemas;

        // For each data item, either place it into the new datastore or move it to a dif node.
        for (const auto &entry : mLocalData) {
            size_t keyHash = hash<string>()(entry.first);

            if (mPreparedView->isResponsibleFor(keyHash)) {
                lock_guard<mutex> prepDataLock(mPreparedDatastoreMut);
                mPreparedDatastore->insert(entry);
            } else {
                // The message is version&key&value. Ampersands in key and value are escaped by
                // backslashes.
                string messageBody = to_string(mPreparedView->scheme().version()) + "&" +
                                     escapeChars(entry.first, "&") + "&" +
                                     escapeChars(dataVersionToString(entry.second), "&");

                // This semaphore will be raised when this piece of data has been successfully
                // moved.
                SemaphorePtr sema = make_shared<Semaphore>(0);
                moveSemas.push_back(sema);

                auto onResult = [sema](Pistache::Http::Response response) {
                    if (response.code() == Pistache::Http::Code::Ok) {
                        sema->up();
                        return true;
                    } else {
                        return false;
                    }
                };

                const ShardInfo &targetShard =
                    mPreparedView->scheme().getResponsibleShardInfo(keyHash);
                const set<string> &targetAddresses = targetShard.getNodeSet();

                sendToRandomNodeUntilSuccess(targetAddresses, "shards/move", messageBody, onResult);
            }
        }

        // Wait for all data to be moved.
        for (SemaphorePtr sema : moveSemas)
            sema->down();
    }

    {
        // Down the mViewsReadSema while we change mView and mPreparedView.
        mViewsReadChangeMut.lock();
        SemaphoreDownGuard viewGuard(mViewsReadSema);
        mViewsReadChangeMut.unlock();

        // Start using the new view and datastore.
        lock_guard<mutex> prepDataLock(mPreparedDatastoreMut);

        mView = move(mPreparedView);
        mLocalData = move(*mPreparedDatastore);

        mPreparedView = nullptr;
        mPreparedDatastore = nullptr;
    }

    // Semaphore was lowered in tryDown().
    mReshardSwitchingSema.up();

    return true;
}

bool
Node::reshardMove(int schemeVersion, const std::string &key, const DataVersion &data)
{
    // Prevent mView and mPreparedView from changing during this method.
    mViewsReadChangeMut.lock();
    SemaphoreDecrementGuard semaGuard(mViewsReadSema);
    mViewsReadChangeMut.unlock();

    if (mView->scheme().version() == schemeVersion) {
        lock_guard<mutex> localDataLock(mLocalDataMut);
        insertOrReplace(mLocalData, key, data);
        return true;
    } else if (mPreparedView->scheme().version() == schemeVersion) {
        lock_guard<mutex> prepDataLock(mPreparedDatastoreMut);
        insertOrReplace(*mPreparedDatastore, key, data);
        return true;
    } else {
        return false;
    }
}

void
Node::updateShardScheme(const ShardScheme &newScheme)
{
    const size_t GRACE_PERIOD = 100;

    shared_ptr<atomic<bool>> shouldStopPrepare = make_shared<atomic<bool>>(false);
    AtomicVectorPtr<pair<size_t, string>> readyNodes =
        make_shared<AtomicVector<pair<size_t, string>>>();

    SemaphoreList shardPrepareSemas =
        updateShardSchemePrepare(newScheme, shouldStopPrepare, readyNodes);

    // Wait a little to give nodes time to respond.
    this_thread::sleep_for(chrono::milliseconds(GRACE_PERIOD));

    // Wait for some node from every shard to prepare.
    for (int idx = 0; idx < shardPrepareSemas.size(); ++idx)
        shardPrepareSemas[idx]->down();

    shared_ptr<atomic<bool>> shouldStopSwitch = make_shared<atomic<bool>>(false);

    SemaphoreList shardSwitchSemas =
        updateShardSchemeSwitch(newScheme.version(), shouldStopSwitch, readyNodes);

    // Wait a little to give nodes time to respond.
    this_thread::sleep_for(chrono::milliseconds(GRACE_PERIOD));

    // Wait for some node from every shard to switch.
    for (int idx = 0; idx < shardSwitchSemas.size(); ++idx)
        shardSwitchSemas[idx]->down();

    *shouldStopPrepare = true;
    *shouldStopSwitch = true;
}

Node::SemaphoreList
Node::updateShardSchemePrepare(const ShardScheme &newScheme, AtomicBoolPtr shouldStopPrepare,
                               AtomicVectorPtr<pair<size_t, string>> readyNodes)
{
    const string prepareString = ShardSchemeUtility::serializeScheme(newScheme);

    SemaphoreList shardPrepareSemas;

    for (size_t id = 0; id < newScheme.getNumShards(); ++id) {
        SemaphorePtr shardSema = make_shared<Semaphore>(0);
        shardPrepareSemas.push_back(shardSema);

        const ShardInfo &shardInfo = newScheme.getShardInfo(id);

        const auto &shardNodes = shardInfo.getNodeSet();

        for (const string &nodeAddr : shardNodes) {
            // Begin sending out PREPARE requests.
            sendUntilSuccess(nodeAddr, "shards/prepare", prepareString,
                             [=](Pistache::Http::Response response) {
                                 if (parsePrepareSuccess(response)) {
                                     readyNodes->emplace_back(id, nodeAddr);
                                     shardSema->up();
                                     return true;
                                 } else {
                                     return false;
                                 }
                             },
                             shouldStopPrepare);
        }
    }

    return shardPrepareSemas;
}

Node::SemaphoreList
Node::updateShardSchemeSwitch(int newVersion, AtomicBoolPtr shouldStopSwitch,
                              AtomicVectorPtr<pair<size_t, string>> readyNodes)
{
    const string switchString = to_string(newVersion);

    SemaphoreList shardSwitchSemas;

    // Figure out how many shards there are.
    size_t numShards = 0;
    for (int readyIdx = 0; readyIdx < readyNodes->size(); ++readyIdx) {
        const auto &readyPair = (*readyNodes)[readyIdx];
        if (readyPair.first >= numShards)
            numShards = readyPair.first + 1;
    }

    // Initialize all semaphores to 0.
    for (size_t id = 0; id < numShards; ++id)
        shardSwitchSemas.emplace_back(make_shared<Semaphore>(0));

    for (int readyIdx = 0; readyIdx < readyNodes->size(); ++readyIdx) {
        size_t id = (*readyNodes)[readyIdx].first;
        const std::string &address = (*readyNodes)[readyIdx].second;

        sendUntilSuccess(address, "shards/switch", switchString,
                         [=](Pistache::Http::Response response) {
                             if (parseSwitchSuccess(response)) {
                                 shardSwitchSemas[id]->up();
                                 return true;
                             } else {
                                 return false;
                             }
                         },
                         shouldStopSwitch);
    }

    return shardSwitchSemas;
}

void
Node::sendUntilSuccess(const string &address, const string &resource, const string &body,
                       function<bool(Pistache::Http::Response)> onResult, AtomicBoolPtr shouldStop)
{
    thread t = thread([=]() {
        bool gotSuccess = false;

        auto responseLambda = [onResult, &gotSuccess](Pistache::Http::Response response) {
            gotSuccess = onResult(response);
        };

        while (!gotSuccess && (shouldStop == nullptr || !(*shouldStop))) {
            auto rsp = mView->sendMsg(address, resource, body, chrono::milliseconds(1000));
            rsp.then(responseLambda, Pistache::Async::IgnoreException);

            Pistache::Async::Barrier barrier(rsp);
            barrier.wait();
        }
    });

    // For some reason clang-format was doing a weird thing if you just wrote thread(...).detach();
    t.detach();
}

void
Node::sendToRandomNodeUntilSuccess(const set<string> &addresses, const string &resource,
                                   const string &body,
                                   function<bool(Pistache::Http::Response)> onResult,
                                   AtomicBoolPtr shouldStop)
{
    // TODO: This is almost identical to sendUntilSuccess(), but calling this in sendUntilSuccess
    // feels wrong.

    thread t = thread([=]() {
        bool gotSuccess = false;

        auto responseLambda = [onResult, &gotSuccess](Pistache::Http::Response response) {
            gotSuccess = onResult(response);
        };

        auto addressItr = addresses.begin();
        assert(addressItr != addresses.end());

        while (!gotSuccess && (shouldStop == nullptr || !(*shouldStop))) {
            const string &address = *addressItr;

            ++addressItr;
            if (addressItr == addresses.end())
                addressItr = addresses.begin();

            auto rsp = mView->sendMsg(address, resource, body, chrono::milliseconds(1000));
            rsp.then(responseLambda, Pistache::Async::IgnoreException);

            Pistache::Async::Barrier barrier(rsp);
            barrier.wait();
        }
    });

    t.detach();
}

void
Node::syncThread()
{
    while (true) {
        this_thread::sleep_for(chrono::milliseconds(SYNC_PERIOD));

        mViewsReadChangeMut.lock();
        SemaphoreDecrementGuard viewsReadGuard(mViewsReadSema);
        mViewsReadChangeMut.unlock();

        optional<size_t> shardIdOpt = mView->getShardId();

        // If this node is not in a shard, do nothing. This can happen during the initialization
        // of the distributed system.
        if (!shardIdOpt)
            continue;

        const ShardInfo &shard = mView->scheme().getShardInfo(shardIdOpt.value());

        const set<string> &nodes = shard.getNodeSet();

        // This node should at least be in the set, otherwise something has gone wrong.
        assert(!nodes.empty());

        unsigned nodeToSync = rand() % nodes.size();

        auto it = nodes.begin();
        while (nodeToSync != 0) {
            ++it;
            --nodeToSync;
        }

        // If I choose myself, just loop back around
        if ((*it) == mView->getAddress()) {
            this_thread::sleep_for(chrono::milliseconds(SYNC_SALT));
            continue;
        }

        mLocalDataMut.lock();
        string dataString = mapToDataString(mLocalData);
        mLocalDataMut.unlock();

        mView->sendMsg(*it, "dataSync/push", dataString);
        // we don't care about the results, just loop back around
    }
}

void
Node::incrementClock()
{
    mNodeClock = VectorClock::add(mNodeClock, mView->getAddress(), 1);
}

void
Node::mergeAndIncrementClock(const VectorClock &other)
{
    mNodeClock = VectorClock::add(VectorClock::merge(mNodeClock, other), mView->getAddress(), 1);
}

void
Node::mergeClock(const VectorClock &other)
{
    mNodeClock = VectorClock::merge(mNodeClock, other);
}

void
Node::waitForNewSchemeVersion(int newVersion)
{
    unique_lock<mutex> lk(mSchemeChangeMut);
    while (mView->scheme().version() < newVersion)
        mSchemeChangeCV.wait(lk);
}

void
Node::triggerSchemeChangeEnd()
{
    unique_lock<mutex> lk(mSchemeChangeMut);
    mSchemeChangeCV.notify_all();
}
