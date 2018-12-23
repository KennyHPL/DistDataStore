#include "ParsingHelpers.h"

#include <algorithm>
#include <cassert>
#include <set>
#include <sstream>

using namespace std;

string
unHTTPify(const string &strWithPluses)
{
    string str = strWithPluses;

    // Replaces all pluses by spaces before we start processing hex codes.
    replace(str.begin(), str.end(), '+', ' ');

    string out;

    int lastPos = 0;
    int p = str.find_first_of('%');
    while (p != string::npos) {
        out += str.substr(lastPos, p - lastPos);

        string code = str.substr(p + 1, 2);
        stringstream ss;
        ss << hex << code;
        int val;
        ss >> val;
        out += (char)val;

        lastPos = p + 3;
        p = str.find_first_of('%', lastPos);
    }

    out += str.substr(lastPos, str.size());

    return out;
}

typename Node::DataVersion
stringToDataVersion(const string &str)
{
    int p = str.find_first_of('|');
    string clock = str.substr(0, p);
    string data = str.substr(p + 1, str.size() - p - 1);

    return Node::DataVersion(move(data), VectorClock::fromString(clock));
}

string
dataVersionToString(const Node::DataVersion &dataVersion)
{
    string ret = dataVersion.clock.toString() + "|";
    ret += dataVersion.value;

    return ret;
}

std::pair<Node::DataVersion, int>
stringTodataVersionAndSchemeVersion(const std::string &str)
{
    int p = str.find_first_of('|');
    string sec = str.substr(0, p);
    string fir = str.substr(p + 1, str.size() - p - 1);

    return {stringToDataVersion(fir), atoi(sec.c_str())};
}

std::string
dataVersionAndSchemeVersionToString(const std::pair<Node::DataVersion, int> &val)
{
    string ret = to_string(val.second) + "|";
    ret += dataVersionToString(val.first);

    return ret;
}

unordered_map<string, typename Node::DataVersion>
dataStringToMap(const string &str)
{
    unordered_map<string, typename Node::DataVersion> ret;

    int lastPos = 0;
    while (true) {
        int p = str.find_first_of('|', lastPos);
        if (p == string::npos)
            break;
        string key = str.substr(lastPos, p - lastPos);
        int end = str.find_first_of('$', p + 1);
        string rest = str.substr(p + 1, end - p - 1);

        lastPos = end + 1;

        ret.emplace(key, stringToDataVersion(rest));
    }

    return ret;
}

string
mapToDataString(const unordered_map<string, typename Node::DataVersion> &map)
{
    string ret;
    for (auto it = map.begin(); it != map.end(); ++it) {
        ret += it->first;
        ret += '|';
        ret += dataVersionToString(it->second);
        ret += '$';
    }

    return ret;
}

/// Removes surrounding whitespace from string.
string
trim(const string &str)
{
    int startIdx = 0;
    int endIdx = str.size();

    while (startIdx <= endIdx && isspace(str[startIdx]))
        ++startIdx;

    while (startIdx < endIdx && isspace(str[endIdx - 1]))
        --endIdx; // safe because endIdx > startIdx >= 0.

    return str.substr(startIdx, endIdx);
}

/// Splits the given string by commas. Trims whitespace.
vector<string>
splitByCommas(const string &str)
{
    using namespace std;
    vector<string> result;

    // Split by commas.
    size_t entryStart = 0;
    size_t nextComma = str.find(',');

    while (nextComma != string::npos) {
        result.push_back(trim(str.substr(entryStart, nextComma - entryStart)));

        entryStart = nextComma + 1;
        nextComma = str.find(',', entryStart);
    }

    if (nextComma > entryStart)
        result.push_back(str.substr(entryStart));

    return result;
}

/// The variable paramString should be in the format "name1=value1&name2=value2&...&nameK=valueK".
/// If paramName is nameJ in the above, this will return valueJ. If the name is not found, an
/// empty string is returned.
string
getParam(const string &paramString, const string &paramName)
{
    using namespace std;

    size_t paramStart = paramString.find(paramName + "=");

    if (paramStart == string::npos)
        // If we don't find the parameter, just return the empty string. We
        // can do optionals later if we want.
        return "";

    else {
        size_t valueStart = paramStart + paramName.size() + 1;

        // Handle case "val=<value>&&payload=<payload>"
        size_t paramDelimIdx = paramString.find('&', valueStart);

        // Case 1:
        //  If we find the && symbol, then paramDelimIdx is a valid index and
        //  paramDelimIdx - valueStart is the length of the value. In the
        //  example above, this would happen if we search for "val".
        // Case 2:
        //  paramDelimIdx is npos which is the largest size_t, and
        //  paramDelimIdx - valueStart is a very large number, so the rest
        //  of the string will be used. In the example above, this would happen
        //  if we search for "payload".

        return paramString.substr(valueStart, paramDelimIdx - valueStart);
    }
}

/// Parses the request body for a parameter.
string
getParam(const Pistache::Http::Request &request, const string &paramName)
{
    return getParam(unHTTPify(request.body()), paramName);
}

string
escapeChars(string_view str, string_view charsWithoutBackslash)
{
    // Make sure to escape backslashes too!
    string chars(charsWithoutBackslash);
    chars += '\\';

    string escaped;

    size_t prev = 0;
    size_t next = str.find_first_of(chars);

    if (next == prev) {
        escaped += '\\';
        escaped += str[0];
        next = str.find_first_of(chars, prev + 1);
    } else if (next == string::npos) {
        escaped += str;
        return escaped;
    }

    do {

        assert(next > prev);

        // Copy all of the characters after prev up to but not including next.
        escaped += str.substr(prev + 1, next - prev - 1);

        // Add the next character and a backslash.
        escaped += '\\';
        escaped += str[next];

        prev = next;
        next = str.find_first_of(chars, prev + 1);
    } while (next != string::npos);

    // Copy the rest.
    escaped += str.substr(prev + 1);

    return escaped;
}

string
unescapeChars(string_view str)
{
    string unescaped;

    size_t processed = 0;
    size_t nextBackslash;

    while ((nextBackslash = str.find('\\', processed)) < str.size()) {
        // Copy all characters starting at index `processed` and going up to
        // but not including the index `nextBackslash`.
        unescaped += str.substr(processed, nextBackslash - processed);

        // Process keeps track of the number of characters processed, which is
        // one more than the index of the last character processed.
        processed = nextBackslash + 1;
    }

    if (processed < str.size())
        unescaped += str.substr(processed);

    return unescaped;
}

size_t
findNextUnescapedChar(string_view str, char lookFor)
{
    return findNextUnescapedString(str, string_view(&lookFor, 1));
}

size_t
findNextUnescapedString(string_view str, string_view lookFor)
{
    size_t location = str.find(lookFor);

    while (location != string::npos && location > 0 && str[location - 1] == '\\')
        location = str.find(lookFor, location + 1);

    return location;
}
