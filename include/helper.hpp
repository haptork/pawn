#if !defined(PAWN_HELPER)
#define PAWN_HELPER

#include <vector>
#include <string>

namespace client { namespace helper {

class positionTeller {
public:
  using vectorCol = std::vector<size_t>;
  using vectorVar = std::vector<std::string>;
  positionTeller(const vectorCol &cols, const vectorVar &vars) : _cols{cols}, _vars{vars} {}
  int operator()(std::string s) const;
  int operator()(size_t i) const;
private:
  const vectorCol &_cols;
  const vectorVar &_vars;
};

bool lexCastPawn(std::vector<std::string> &vstr,
                 std::tuple<std::vector<std::string>, std::vector<double>> &out,
                 std::vector<int> colsString, std::vector<int> colsNumeric,
                 bool strict);

}}

#endif