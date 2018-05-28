#if !defined(PAWN_HELPER)
#define PAWN_HELPER

#include <algorithm>
#include <vector>
#include <set>
#include <map>
#include <string>

#include <boost/algorithm/string.hpp>
#include <ezl/helper/vglob.hpp>

namespace client { namespace helper {

struct ColIndices {
  std::vector<size_t> str;
  std::vector<size_t> num;
  std::vector<std::string> var;
  std::vector<std::string> varStr;
  void add(ColIndices x) {
    std::move(begin(x.str), end(x.str), back_inserter(str));
    std::move(begin(x.num), end(x.num), back_inserter(num));
    std::move(begin(x.var), end(x.var), back_inserter(var));
    std::move(begin(x.varStr), end(x.varStr), back_inserter(varStr));
  }
  void uniq() {
    auto uniq = std::set<size_t>{std::begin(num), std::end(num)};
    num = std::vector<size_t>{std::begin(uniq), std::end(uniq)};
    uniq = std::set<size_t>{std::begin(str), std::end(str)};
    str = std::vector<size_t>{std::begin(uniq), std::end(uniq)};
  }

  void sort() {
    std::sort(std::begin(num), std::end(num));
    std::sort(std::begin(str), std::end(str));
  }
};

struct Global {
  std::map<std::string, std::string> gQueries;
  std::map<std::string, std::string> gVarsS;
  std::map<std::string, double> gVarsN;
};

class positionTeller {
public:
  positionTeller(const ColIndices &cols) : _cols{cols} {}
  int var(std::string s) const;
  int varStr(std::string s) const;
  int str(size_t i) const;
  int num(size_t i) const;
private:
  const ColIndices &_cols;
};

void processHeader(client::helper::ColIndices &x, std::vector<std::string>& h);

std::vector<std::string> headerCols(std::string fnameGlob);

std::string cookDumpHeader(const ColIndices& h);

void print(const ColIndices &colIndices);

bool lexCastPawn(std::vector<std::string> &vstr,
                 std::tuple<std::vector<std::string>, std::vector<double>> &out,
                 std::vector<int> colsString, std::vector<int> colsNumeric,
                 bool strict);

}}

#endif