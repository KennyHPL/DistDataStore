#pragma once

#include "Node.h"
#include "VectorClock.h"

#include <cctype>
#include <pistache/endpoint.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

typename Node::DataVersion stringToDataVersion(const std::string &str);

std::string unHTTPify(const std::string &str);

std::string dataVersionToString(const Node::DataVersion &dataVersion);

std::pair<Node::DataVersion, int> stringTodataVersionAndSchemeVersion(const std::string &str);

std::string dataVersionAndSchemeVersionToString(const std::pair<Node::DataVersion, int> &val);

std::unordered_map<std::string, typename Node::DataVersion> dataStringToMap(const std::string &str);

std::string mapToDataString(const std::unordered_map<std::string, typename Node::DataVersion> &map);

/// Removes surrounding whitespace from string.
std::string trim(const std::string &str);

/// Splits the given string by commas. Trims whitespace.
std::vector<std::string> splitByCommas(const std::string &str);

/// The variable paramString should be in the format "name1=value1&name2=value2&...&nameK=valueK".
/// If paramName is nameJ in the above, this will return valueJ. If the name is not found, an
/// empty string is returned.
std::string getParam(const std::string &paramString, const std::string &paramName);

/// Parses the request body for a parameter.
std::string getParam(const Pistache::Http::Request &request, const std::string &paramName);

/// Prepends a backslash to every character in str that should be escaped (specified by chars).
std::string escapeChars(std::string_view str, std::string_view chars);

/// Inverse of escapeChars().
std::string unescapeChars(std::string_view str);

/// Finds the next instance of lookFor that is not preceded by a backslash. Returns its index if
/// found and returns string::npos if not found.
///
/// @remark Uses std::string_view to be more general.
size_t findNextUnescapedChar(std::string_view str, char lookFor);

/// Finds the next instance of lookFor that is not preceded by a backslash. Returns the index of its
/// first character if found and returns string::npos if not found.
///
/// @remark Uses std::string_view to be more general.
size_t findNextUnescapedString(std::string_view str, std::string_view lookFor);
