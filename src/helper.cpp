#include <algorithm>

#include <helper.hpp>
#include <assert.h>

int client::helper::positionTeller::operator()(std::string s) const {
  auto it = std::find(begin(_vars), end(_vars), s);
  // assert(it != std::end(_vars));
  return _cols.size() + (it - std::begin(_vars));
}

int client::helper::positionTeller::operator()(size_t i) const {
  auto it = std::find(begin(_cols), end(_cols), i);
  // assert(it != std::end(_cols));
  return it - std::begin(_cols);
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
