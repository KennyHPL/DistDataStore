#pragma once

#include <mutex>
#include <vector>

/// A thread-safe resizable vector where elements can only be pushed but not erased.
/// Note that although the subscript operation is thread-safe, no thread-safety is provided for
/// modifying data after accessing it. For example, `vector[idx].nonAtomicVal = 3` is not a
/// thread-safe operation.
template <typename T>
class AtomicVector
{
public:
    size_t size() const
    {
        std::lock_guard<std::mutex> lock(mReadMutex);
        return mVector.size();
    }

    T &operator[](size_t idx)
    {
        std::lock_guard<std::mutex> lock(mReadMutex);
        return mVector[idx];
    }

    const T &operator[](size_t idx) const
    {
        std::lock_guard<std::mutex> lock(mReadMutex);
        return mVector[idx];
    }

    void push_back(const T &val)
    {
        std::lock_guard<std::mutex> writeLock(mWriteMutex);

        // Block reads in case the vector is resized.
        std::lock_guard<std::mutex> readLock(mReadMutex);

        mVector.push_back(val);
    }

    template <typename... Args>
    void emplace_back(Args... args)
    {
        std::lock_guard<std::mutex> writeLock(mWriteMutex);

        // Block reads in case the vector is resized.
        std::lock_guard<std::mutex> readLock(mReadMutex);

        mVector.emplace_back(args...);
    }

private:
    mutable std::mutex mWriteMutex;
    mutable std::mutex mReadMutex;
    std::vector<T> mVector;
};
