#include <algorithm>
#include <tuple>
#include <iostream>
#include <assert.h>

#include <helper.hpp>

int client::helper::positionTeller::var(std::string s) const {
  auto it = std::find(begin(_cols.var), end(_cols.var), s);
  // assert(it != std::end(_vars));
  return _cols.num.size() + (it - std::begin(_cols.var));
}

int client::helper::positionTeller::str(size_t i) const {
  auto it = std::find(begin(_cols.str), end(_cols.str), i);
  // assert(it != std::end(_cols));
  return it - std::begin(_cols.str);
}

int client::helper::positionTeller::num(size_t i) const {
  auto it = std::find(begin(_cols.num), end(_cols.num), i);
  // assert(it != std::end(_cols));
  return it - std::begin(_cols.num);
}

void client::helper::print(const client::helper::ColIndices &colIndices) {
  std::cout << "cols: ";
  std::cout << "(num: ";
  for (auto it : colIndices.num) {
    std::cout << it << ", ";
  }
  std::cout << ") (str: ";
  for (auto it : colIndices.str) {
    std::cout << it << ", ";
  }
  std::cout << ") (var: ";
  for (auto it : colIndices.var) {
    std::cout << it << ", ";
  }
  std::cout << ")";
}

bool client::helper::lexCastPawn(std::vector<std::string> &vstr,
                 std::tuple<std::vector<std::string>, std::vector<double>> &out,
                 std::vector<int> colsString, std::vector<int> colsNumeric,
                 bool strict)
{
  auto i = 0;
  for (auto it : colsString)
  {
    if (vstr[it - 1].empty() && strict)
    {
      return false;
    }
    std::get<0>(out)[i++] = std::move(vstr[it - 1]);
  }
  i = 0;
  for (auto it : colsNumeric)
  {
    if (vstr[it - 1].empty() && strict)
    {
      return false;
    }
    try
    {
      std::get<1>(out)[i] = std::stod(vstr[it - 1]);
      ++i;
    }
    catch (std::invalid_argument ex)
    {
      // Karta::inst().log("Invalid integer value.", LogMode::info);
      if (strict)
        return false;
      std::get<1>(out)[i++] = 0;
    }
    catch (std::out_of_range ex)
    {
      // Karta::inst().log("Out of range integer value.", LogMode::info);
      if (strict)
        return false;
      std::get<1>(out)[i++] = 0;
    }
    catch (...)
    {
      // Karta::inst().log("Unknown integer conversion error.", LogMode::info);
      if (strict)
        return false;
      std::get<1>(out)[i++] = 0;
    }
  }
  return true;
};
