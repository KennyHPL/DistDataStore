#pragma once

#include <time.h>
#include <unordered_map>

class VectorClock
{
public:
    VectorClock() {}

    // copy construct
    VectorClock(const std::unordered_map<std::string, int> &nodeClocks);
    // move construct
    VectorClock(std::unordered_map<std::string, int> &&nodeClocks);

    enum CompareValue
    {
        Equal = 0,
        LessThan = -1,
        GreaterThan = 1,
        Concurrent = 2
    };

    CompareValue compare(const VectorClock &other) const;

    void addNode(const std::string &ip);

    std::string toString() const;

    // both ops make a new VectorClock, with a new time stamp
    static VectorClock merge(const VectorClock &a, const VectorClock &b);
    static VectorClock add(const VectorClock &a, const std::string &index, int value);

    static VectorClock fromString(const std::string &str);

    // is a the max between a and b
    // This breaks concurrency with the time stamp
    static bool isMax(const VectorClock &a, const VectorClock &b);

private:
    std::unordered_map<std::string, int> mNodeClocks;
    time_t mPhysicalTimeStamp;
};
