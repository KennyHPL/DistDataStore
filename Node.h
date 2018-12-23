#pragma once

#include "AtomicVector.h"
#include "Semaphore.h"
#include "VectorClock.h"
#include "View.h"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <pistache/http_headers.h>
#include <string>
#include <unordered_map>
#include <vector>

class Node
{
public:
    template <class T>
    class ClientOpReturnValue
    {
    public:
        // TODO: Put implementation elsewhere.

        /// Creates an empty return value. This should result in a Bad_Request response code.
        ClientOpReturnValue()
            : mIsBadRequest(true)
        {
        }

        ClientOpReturnValue(int newVersion)
            : mIsBadRequest(false)
            , newSchemeVersion(newVersion)
        {
        }

        ClientOpReturnValue(const T &v, const VectorClock &c)
            : mIsBadRequest(false)
            , value(v)
            , clock(c)
            , newSchemeVersion(-1)
        {
        }
        ClientOpReturnValue(T &&v, VectorClock &&c)
            : mIsBadRequest(false)
            , value(v)
            , clock(c)
            , newSchemeVersion(-1)
        {
        }

        bool hasWrongSchemeVersion() const { return newSchemeVersion != -1; }
        bool isBadRequest() const { return mIsBadRequest; }

        // TODO: Encapsulate.
        T value;
        VectorClock clock;
        int newSchemeVersion;

    private:
        bool mIsBadRequest;
    };

    struct DataVersion
    {
        DataVersion(const std::string &val, const VectorClock &clock)
            : value(val)
            , clock(clock)
        {
        }

        std::string value;
        VectorClock clock;
    };

    using DataStore = std::unordered_map<std::string, DataVersion>;

    enum class PutSuccessType
    {
        CreatedNewValue,
        UpdatedExistingValue,
        KeyNotValid,
        ObjectTooLarge
    };

    Node(std::shared_ptr<View> view);

    // CLIENT: Key-Value Store operations:
    ClientOpReturnValue<PutSuccessType>
    putElement(const std::string &key, const std::string &value, const VectorClock &payload);

    ClientOpReturnValue<std::optional<std::string>>
    getElement(const std::string &key, const VectorClock &payload);

    ClientOpReturnValue<bool> hasElement(const std::string &key, const VectorClock &payload);

    ClientOpReturnValue<bool> delElement(const std::string &key, const VectorClock &payload);

    size_t count();

    // CLIENT: View operations:
    bool addNode(const std::string &ipPort);

    bool delNode(const std::string &ipPort);

    std::shared_ptr<View> getView() const { return mView; }

    /// Attempts to create a new shard scheme with the given number of shards and to propagate
    /// it to other nodes. Returns false if there are too many shards, and otherwise returns
    /// true once it is likely that most nodes have updated their scheme.
    bool reshard(size_t numShards);

    // INTERSERVER: key-value store operations:
    // returns the data version, and the scheme version
    std::optional<std::pair<DataVersion, int>> directGet(const std::string &key);

    bool syncData(const std::string &data);

    /// Prepares for a view change. Returns true on success, false on failure.
    bool reshardPrepare(const ShardScheme &scheme);

    /// Attempts to switch to the new scheme version. Returns true on success, false on failure.
    bool reshardSwitch(int schemeVersion);

    /// Attempts to move the data to this node. Returns true on success, false on failure.
    bool reshardMove(int schemeVersion, const std::string &key, const DataVersion &data);

    // Forwarding
    Pistache::Http::RequestBuilder requestBuilder() { return mView->requestBuilder(); }
    std::string keyToNode(const std::string &key) const;

    void waitForNewSchemeVersion(int newVersion);

private:
    /// Propagates the new shard scheme through the system. Must always succeed.
    void updateShardScheme(const ShardScheme &newScheme);

    using AtomicBoolPtr = std::shared_ptr<std::atomic<bool>>;
    using SemaphorePtr = std::shared_ptr<Semaphore>;
    using SemaphoreList = std::vector<SemaphorePtr>;

    template <typename T>
    using AtomicVectorPtr = std::shared_ptr<AtomicVector<T>>;

    /// Performs the prepare phase of resharding. This will send a shards/prepare message to
    /// every node in the system. This will return a list of semaphores that will count the number
    /// of nodes in each shard that have responded with success. When a node responds with success,
    /// it will be added to readyNodes its shard's semaphore will be raised.
    SemaphoreList
    updateShardSchemePrepare(const ShardScheme &newScheme, AtomicBoolPtr shouldStopPrepare,
                             AtomicVectorPtr<std::pair<size_t, std::string>> readyNodes);

    /// Performs the switch phase of reshading. This will send a shards/switch message to
    /// every node in readyNodes (it is assumed that readyNodes can increase in size but its
    /// existing entries will not change). This will return a list of semaphores that count the
    /// number of nodes in each shard that have responded with success. It is assumed that
    /// readyNodes has an entry for each shard ID at the start of this function.
    SemaphoreList
    updateShardSchemeSwitch(int newVersion, AtomicBoolPtr shouldStopSwitch,
                            AtomicVectorPtr<std::pair<size_t, std::string>> readyNodes);

    /// Starts a thread that will send messages to the address. On timeout (or exception), it
    /// will retry unless shouldStop is true. If a response is received, it calls onResult().
    /// The function onResult should return true if it accepts the response and false if it does
    /// not, in which case the message will be resent while shouldStop is false.
    void sendUntilSuccess(const std::string &address, const std::string &resource,
                          const std::string &body,
                          std::function<bool(Pistache::Http::Response)> onResult,
                          AtomicBoolPtr shouldStop = nullptr);

    /// Starts a thread that sends messages to random nodes in the shard. On timeout (or exception),
    /// it will retry unless shouldStop is true. If a response is received, it calls onResult().
    /// The function onResult should return true if it accepts the response and false if it does
    /// not, in which case the message will be resent while shouldStop is false.
public:
    void sendToRandomNodeUntilSuccess(const std::set<std::string> &addresses,
                                      const std::string &resource, const std::string &body,
                                      std::function<bool(Pistache::Http::Response)> onResult,
                                      AtomicBoolPtr shouldStop = nullptr);

private:
    // periodicly chooses a random other thread to send my data to, for them to sync up
    void syncThread();

    void incrementClock();
    void mergeAndIncrementClock(const VectorClock &other);
    void mergeClock(const VectorClock &other);

    void triggerSchemeChangeEnd();

    VectorClock mNodeClock;
    std::shared_ptr<View> mView;
    DataStore mLocalData;

    /// Only one client operation at a time. Lock claimed at the start of each high level client op,
    /// and released at the very end. Don't need to use this lock on lower level ops (inter-server)
    std::mutex mClientOperationMut;

    /// Used to protect mLocalData.
    std::mutex mLocalDataMut;

    /// Used to protect mPreparedDatastore.
    std::mutex mPreparedDatastoreMut;

    /// Decrement for reading mView or mPreparedView, down for writing. Before touching this,
    /// lock mViewsReadChangeSema and unlock it immediately after.
    Semaphore mViewsReadSema;

    /// Lock this while changing mViewsReadSema.
    std::mutex mViewsReadChangeMut;

    // Used to trigger threads waiting on the scheme to be updated
    // Lock exclusivly used for this purpose
    std::mutex mSchemeChangeMut;
    std::condition_variable mSchemeChangeCV;

    /// Whether a reshard-switch is currently being performed.
    Semaphore mReshardSwitchingSema;

    std::unique_ptr<View> mPreparedView;
    std::unique_ptr<DataStore> mPreparedDatastore;
};
