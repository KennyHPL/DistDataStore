#include "VectorClock.h"

#include <algorithm>
#include <assert.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <stdlib.h>
#include <string.h>

using namespace std;

VectorClock::VectorClock(const unordered_map<string, int> &nodeClocks)
    : mPhysicalTimeStamp(time(NULL))
    , mNodeClocks(nodeClocks)
{
}

VectorClock::VectorClock(unordered_map<string, int> &&nodeClocks)
    : mPhysicalTimeStamp(time(NULL))
    , mNodeClocks(nodeClocks)
{
}

VectorClock::CompareValue
VectorClock::compare(const VectorClock &other) const
{
    int sign = 0;

    unordered_set<string> keys;

    for (auto it = mNodeClocks.begin(); it != mNodeClocks.end(); ++it)
        keys.insert(it->first);
    for (auto it = other.mNodeClocks.begin(); it != other.mNodeClocks.end(); ++it)
        keys.insert(it->first);

    for (const string &key : keys) {
        auto ita = mNodeClocks.find(key);
        auto itb = other.mNodeClocks.find(key);

        int a = ita == mNodeClocks.end() ? 0 : ita->second;
        int b = itb == other.mNodeClocks.end() ? 0 : itb->second;

        int d = a - b;
        int newSign = (d > 0) - (d < 0);

        if (sign == 0) sign = newSign;
        else if (newSign == 0 || newSign == sign) continue;
        else return Concurrent;
    }

    // if they are equal, we default to time stamp value
    if (sign == 0)
        sign = (mPhysicalTimeStamp > other.mPhysicalTimeStamp) -
               (mPhysicalTimeStamp < other.mPhysicalTimeStamp);

    return (CompareValue)sign;
}

void
VectorClock::addNode(const string &ip)
{
    mNodeClocks[ip];
}

string
VectorClock::toString() const
{
    struct tm *tmp = localtime(&mPhysicalTimeStamp);
    char physicalTime[200];
    strftime(physicalTime, 200, "PhysicalTime:%D:%H:%M:%S", tmp);

    string ret;
    ret += physicalTime;

    for (auto it = mNodeClocks.begin(); it != mNodeClocks.end(); ++it) {
        ret += " ";
        ret += it->first;
        ret += ";";
        ret += to_string(it->second);
    }
    return ret;
}

VectorClock
VectorClock::merge(const VectorClock &a, const VectorClock &b)
{
    VectorClock r(a.mNodeClocks);

    for (auto it = b.mNodeClocks.cbegin(); it != b.mNodeClocks.cend(); ++it) {
        int bV = it->second;

        auto ita = a.mNodeClocks.find(it->first);

        int aV = ita == a.mNodeClocks.end()? 0 : ita->second;

        r.mNodeClocks[it->first] = max(aV, bV);
    }

    return r;
}

VectorClock
VectorClock::add(const VectorClock &a, const string &index, int value)
{
    VectorClock r(a.mNodeClocks);

    r.mNodeClocks[index] += value;

    return r;
}

VectorClock
VectorClock::fromString(const std::string &str)
{
    if (str.empty()) return VectorClock();

    vector<string> pairs;

    int lastPos = 0;
    while (true) {
        int p = str.find_first_of(' ', lastPos);
        if (p == string::npos) {
            pairs.push_back(str.substr(lastPos, str.size() - lastPos));
            break;
        }
        pairs.push_back(str.substr(lastPos, p - lastPos));
        lastPos = p + 1;
    }

    struct tm tmp;
    memset(&tmp, 0, sizeof(struct tm));
    strptime(pairs[0].c_str(), "PhysicalTime:%D:%H:%M:%S", &tmp);

    time_t timeStamp = mktime(&tmp);

    unordered_map<string, int> nodeClocks;

    for (int i = 1; i < pairs.size(); ++i) {
        const string &pr = pairs[i];
        int p = pr.find_first_of(';');
        nodeClocks[pr.substr(0, p)] = atoi(pr.substr(p + 1, pr.size() - p - 1).c_str());
    }

    VectorClock vc(move(nodeClocks));
    vc.mPhysicalTimeStamp = timeStamp;

    return vc;
}

bool VectorClock::isMax(const VectorClock &a, const VectorClock &b)
{
    CompareValue cv = a.compare(b);

    if (cv == LessThan) return false;
    if (cv == GreaterThan) return true;
    if (b.mPhysicalTimeStamp > a.mPhysicalTimeStamp) return false;
    return true;
}
