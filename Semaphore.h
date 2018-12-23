#pragma once

#include <condition_variable>
#include <mutex>

class Semaphore
{
public:
    Semaphore(int initialCount)
        : mMutex()
        , mConditionVariable()
        , mCount(initialCount)
    {
    }

    /// If the semaphore's count is positive, decrements it and returns true. Otherwise
    /// returns false immediately.
    bool tryDown()
    {
        std::unique_lock<std::mutex> lock(mMutex);

        if (mCount > 0) {
            --mCount;
            return true;
        } else {
            return false;
        }
    }

    /// Waits until the semaphore has positive count, but does not lower it. Equivalent
    /// to calling down() then up() immediately.
    void wait()
    {
        down();
        up();
    }

    /// Decrements the semaphore, possibly making its count negative. Does not block.
    /// To increment, use up().
    void decrement()
    {
        std::unique_lock<std::mutex> lock(mMutex);

        --mCount;
    }

    void down()
    {
        std::unique_lock<std::mutex> lock(mMutex);

        while (!(mCount > 0))
            // wait() releases the lock, which is why this does not cause a deadlock
            mConditionVariable.wait(lock);

        --mCount;
    }

    void up()
    {
        std::lock_guard<std::mutex> lock(mMutex);

        ++mCount;
        mConditionVariable.notify_one();
    }

private:
    std::mutex mMutex;
    std::condition_variable mConditionVariable;
    volatile int mCount;
};

/// Downs the semaphore and raises it when out of scope. Constructor will block
/// because it will call down().
class SemaphoreDownGuard
{
public:
    SemaphoreDownGuard(Semaphore &sema)
        : mSema(sema)
    {
        mSema.down();
    }

    ~SemaphoreDownGuard() { mSema.up(); }

private:
    Semaphore &mSema;
};

/// Decrements the semaphore and raises it when out of scope.
class SemaphoreDecrementGuard
{
public:
    SemaphoreDecrementGuard(Semaphore &sema)
        : mSema(sema)
    {
        mSema.decrement();
    }

    ~SemaphoreDecrementGuard() { mSema.up(); }

private:
    Semaphore &mSema;
};
