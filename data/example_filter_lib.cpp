#include <vector>
#include <string>

extern "C" bool fn(const std::vector<std::string>& v1, const std::vector<double>& v2) {
  if (v2.size() < 2) return false;
  return v2[1] > 5.0;
}