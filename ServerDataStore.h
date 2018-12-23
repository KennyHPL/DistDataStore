#pragma once

#include "MemoryDataStore.h"
#include "Node.h"
#include "VectorClock.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>

class ServerDataStore : public AbstractDataStore<std::pair<std::string, VectorClock>, std::string>
{
public:
    ServerDataStore(std::shared_ptr<Node> node)
        : mNode(node)
    {
        startSync();
    }

    // Modification:
    DataStore::InsertResult
    insert(const std::pair<std::string, VectorClock> &key, const std::string &value) override;

    // Remove an element from the store. If the element doesn't exist, nothing
    // is done.
    bool remove(const std::pair<std::string, VectorClock> &key) override;

    void clear() override;

    void addNode(std::string index);

    // Access:
    std::optional<std::string> get(const std::pair<std::string, VectorClock> &key) override;

    bool contains(const std::pair<std::string, VectorClock> &key) override;

    unsigned count() override;

    std::optional<std::pair<std::string, VectorClock>> directGet(const std::string &key);

    bool syncData(const std::string &data);

    // for when another thread wants to sync
    // return false if isSyncing
    bool trySync()
    {
        if (mIsSyncing)
            return false;
        mSyncWaitCounter = 0;
        return mIsSyncing = true;
    }

private:
    void startSync();

    // run in an infinite loop in the backgrond
    // pop an IP off of mNeedToSync, and send all data
    void syncThread();

    // add all nodes to mNeedToSync
    void dataStateChanged();

    std::shared_ptr<Node> mNode;
    MemoryDataStore<std::string, std::pair<std::string, VectorClock>> mLocalDataStore;

    std::mutex mSyncMut;
    std::mutex mServerMut;
    std::vector<std::string> mNeedToSync;
    std::atomic<bool> mIsSyncing;
    int mSyncWaitCounter;
};
