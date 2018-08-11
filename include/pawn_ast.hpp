/*=============================================================================
    Copyright (c) 2001-2011 Joel de Guzman

    Distributed under the Boost Software License, Version 1.0. (See accompanying
    file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
=============================================================================*/
#if !defined(PAWN_AST_HPP)
#define PAWN_AST_HPP

#include <boost/config/warning_disable.hpp>
#include <boost/fusion/include/adapt_struct.hpp>
#include <boost/fusion/include/io.hpp>
#include <boost/optional.hpp>
#include <boost/variant/recursive_variant.hpp>

#include <list>
#include <map>

#include <aast.hpp>
#include <helper.hpp>
#include <last.hpp>
#include <lcast.hpp>
#include <mast.hpp>

namespace client {
namespace pawn {
namespace ast {
using ColIndices = client::helper::ColIndices;
using uint_ = unsigned int;
using mathExpr = client::math::ast::expr;
using logicalExpr = client::logical::ast::expr;
using logicalCmd = client::logicalc::ast::expr;
using reduceExpr = client::reduce::ast::expr;

struct src {
  std::string fname;
  ColIndices colIndices;
  int index;
};

using quoted_stringT = std::string;

using identifierT = std::string;
using strOperand = boost::variant<identifierT, unsigned int>;

struct map {
  identifierT identifier;
  mathExpr operation;
};

using filter = boost::variant<logicalExpr, logicalCmd>;

struct reduce {
  std::vector<strOperand> cols;
  reduceExpr operation;
  ColIndices colIndices;
};

struct zipExpr;

using unit =
    boost::variant<map, filter, reduce, boost::recursive_wrapper<zipExpr>>;

struct zipExpr {
  std::vector<strOperand> cols;
  src first;
  std::list<unit> units;
  ColIndices colIndices;
  int zipCount;
};

using numSrc = boost::variant<identifierT, uint_>;

struct saveNum {
  numSrc src;
  identifierT dest;
};

struct saveStr {
  strOperand src;
  identifierT dest;
};

struct fileName {
  std::string name;
};

struct queryName {
  std::string name;
};

using saveVal = std::vector<boost::variant<saveNum, saveStr>>;

using terminal = boost::variant<queryName, saveVal, fileName>;

struct expr {
  src first;
  std::list<unit> units;
  terminal last;
  ColIndices colIndices;
  int zipCount{0};
};

struct printer {
  typedef void result_type;

  void operator()(map const &m) const {
    client::math::ast::printer mathPrint;
    std::cout << "map " << m.identifier << " to ";
    mathPrint(m.operation);
    std::cout << " | ";
  }

  void operator()(logicalExpr const &f) const {
    client::logical::ast::printer logicalPrint;
    std::cout << "filter with expr ";
    logicalPrint(f);
    std::cout << " | ";
  }

  void operator()(logicalCmd const &f) const {
    client::logicalc::ast::printer logicalPrint;
    std::cout << "filter with cmd ";
    logicalPrint(f);
    std::cout << " | ";
  }

  void operator()(filter const &f) const {
    boost::apply_visitor(*this, f);
  }

  struct printStrOperand {
    using result_type = void;
    result_type operator()(const std::string &x) const {
      std::cout << x;
    }
    result_type operator()(const unsigned int &x) const {
      std::cout << x;
    }
  };

  void operator()(zipExpr const &z) const {
    std::cout << "zip ";
    if (!z.cols.empty()) {
      std::cout << "group by ";
      for (auto& it : z.cols) {
        boost::apply_visitor(printStrOperand{}, it);
      }
    }
    std::cout << "(";
    std::cout << z.first.index << ": file " << z.first.fname << " | ";
    client::helper::print(z.first.colIndices);
    for (const auto &it : z.units) {
      boost::apply_visitor(*this, it);
    }
    std::cout << ")";
    client::helper::print(z.colIndices);
    std::cout << " | ";
  }

  void operator()(reduce const &r) const {
    client::reduce::ast::printer reducePrint;
    std::cout << "reduce to ";
    reducePrint(r.operation);
    if (!r.cols.empty()) {
      std::cout << " group by ";
      for (auto& it : r.cols) {
        boost::apply_visitor(printStrOperand{}, it);
        std::cout << ", ";
      }
    }
    client::helper::print(r.colIndices);
    std::cout << " | ";
  }

  void operator()(const saveNum &s) const {
    std::cout << " saveNum from ";
    boost::apply_visitor(printStrOperand{}, s.src);
    std::cout << " to " << s.dest;
  }

  void operator()(const saveStr &s) const {
    std::cout << " saveStr from ";
    boost::apply_visitor(printStrOperand{}, s.src);
    std::cout << " to " << s.dest;
  }

  void operator()(saveVal const &s) const {
    std::cout << "saveVal ";
    for (const auto &it : s) {
      boost::apply_visitor(*this, it);
    }
  }

  void operator()(queryName const &q) const {
    std::cout << " saveQuery as " << q.name;
  }

  void operator()(fileName const &f) const {
    std::cout << " saveFile as " << f.name;

  }

  void operator()(expr const &x) const {
    std::cout << x.first.index << ": file " << x.first.fname << " | ";
    client::helper::print(x.first.colIndices);
    for (const auto &it : x.units) {
      boost::apply_visitor(*this, it);
    }
    boost::apply_visitor(*this, x.last);
    std::cout << "zipC: " << x.zipCount;
  }
};

///////////////////////////////////////////////////////////////////////////
//  The cols evaluator
///////////////////////////////////////////////////////////////////////////
struct colsEval {
private:
  using ColIndices = client::helper::ColIndices;
  using Global = client::helper::Global;

  const Global &_global;
  ColIndices _cur;
  ColIndices *_pre {nullptr};
  client::math::ast::colsEval _meval;
  client::logical::ast::colsEval _leval;
  client::reduce::ast::colsEval _aeval;
  client::logicalc::ast::colsEval _lcmd;
  enum class state : int { none, first, many };
  state _st{state::none};
  int _zipCount = 0;
  std::vector<std::string> _headers;

  struct colsOperand {
    using result_type = std::pair<unsigned int, std::string>;
    const std::vector<std::string>& _headers;
    const std::vector<std::string>& _vars;
    colsOperand(const std::vector<std::string>& headers, const std::vector<std::string>& vars) : _headers{headers}, _vars{vars} {}
    result_type operator()(const std::string &var) const {
      auto jt = std::find(begin(_headers), end(_headers), var);
      if (jt != std::end(_headers)) return (*this)((unsigned int)(jt - std::begin(_headers)) + 1);
      auto it = std::find(begin(_vars), end(_vars), var);
      if (it == std::end(_vars)) return std::make_pair(0, "Error: " + var + " used before declaration.");
      return std::make_pair(0, "");
    }
    result_type operator()(unsigned int col) const { return std::make_pair(col, ""); }
  };

  struct chekStrOperand {
    using result_type = std::pair<unsigned int, std::string>;
    const ColIndices& _pre;
    const std::vector<std::string>& _headers;
    chekStrOperand(const ColIndices& pre, const std::vector<std::string>& headers) : _pre{pre}, _headers{headers} {}
    result_type operator()(const std::string &var) const {
      auto jt = std::find(begin(_headers), end(_headers), var);
      if (jt != std::end(_headers)) return (*this)((unsigned int)(jt - std::begin(_headers)) + 1);
      auto it = std::find(begin(_pre.varStr), end(_pre.varStr), var);
      if (it == std::end(_pre.varStr)) return std::make_pair(0, "Error: %" + var + " used before declaration.");
      return std::make_pair(0, "");
    }
    result_type operator()(unsigned int col) const { 
      auto it = std::find(begin(_pre.str), end(_pre.str), col);
      if (it == std::end(_pre.str)) return std::make_pair(0, "Error: %" + std::to_string(col) + " used before declaration.");
      return std::make_pair(col, ""); 
    }
  };

  struct chekNumOperand {
    using result_type = std::pair<unsigned int, std::string>;
    const ColIndices& _pre;
    const std::vector<std::string>& _headers;
    chekNumOperand(const ColIndices& pre, const std::vector<std::string>& headers) : _pre{pre}, _headers{headers} {}
    result_type operator()(const std::string &var) const {
      auto jt = std::find(begin(_headers), end(_headers), var);
      if (jt != std::end(_headers)) return (*this)((unsigned int)(jt - std::begin(_headers)) + 1);
      auto it = std::find(begin(_pre.var), end(_pre.var), var);
      if (it == std::end(_pre.var)) return std::make_pair(0, "Error: %" + var + " used before declaration.");
      return std::make_pair(0, "");
    }
    result_type operator()(unsigned int col) const { 
      auto it = std::find(begin(_pre.num), end(_pre.num), col);
      if (it == std::end(_pre.num)) return std::make_pair(0, "Error: %" + std::to_string(col) + " used before declaration.");
      return std::make_pair(col, ""); 
    }
  };

  struct terminalCheck {
  private:
    ColIndices &_pre;
    const std::vector<std::string>& _headers;
    bool _isInitial;
  public:
    using result_type = std::pair<ColIndices, std::string>;
    terminalCheck(ColIndices &cols, const std::vector<std::string>& headers, bool isInitial) 
        : _pre{cols}, _headers{headers}, _isInitial{isInitial} {}
    result_type operator()(const queryName &) const { return result_type{}; }
    result_type operator()(const fileName &) const { return result_type{}; }
    result_type operator()(const saveVal &s) const {
      ColIndices res;
      for (auto &it : s) {
        auto x = boost::apply_visitor(*this, it);
        if (x.second.size() > 0) return x;
        res.add(x.first);
      }
      return std::make_pair(res, "");
    }
    result_type operator()(const saveNum &s) const {
      ColIndices res;
      if (_isInitial) {
        auto x = boost::apply_visitor(colsOperand{_headers, _pre.var}, s.src);
        if (x.second.size() > 0) return std::make_pair(res, x.second);
        if (x.first) res.num.push_back(x.first);
        return std::make_pair(res, "");
      }
      auto x = boost::apply_visitor(chekNumOperand{_pre, _headers}, s.src);
      return std::make_pair(res, x.second);
    }
    result_type operator()(const saveStr &s) const {
      ColIndices res;
      if (_isInitial) {
        auto x = boost::apply_visitor(colsOperand{_headers, _pre.varStr}, s.src);
        if (x.second.size() > 0) return std::make_pair(res, x.second);
        if (x.first) res.str.push_back(x.first);
        return std::make_pair(res, "");
      }
      auto x = boost::apply_visitor(chekStrOperand{_pre, _headers}, s.src);
      return std::make_pair(res, x.second);
    }
  };


public:
  typedef std::string result_type;
  colsEval(const Global &global)
      : _global{global}, _cur{}, _meval{_cur, _global},
        _leval{_cur, _global}, _aeval{_cur}, _lcmd{} {}

  result_type operator()(map const &m) {
    auto i = std::find(std::begin(_cur.var), std::end(_cur.var), m.identifier);
    if (i != std::end(_cur.var)) return "Err: " + m.identifier + " redeclared.";
    auto j = _global.gVarsN.find(m.identifier);
    if (j != std::end(_global.gVarsN)) return "Err: " + m.identifier + " is already used in global vars.";
    auto k = std::find(std::begin(_headers), std::end(_headers), m.identifier);
    if (k != std::end(_headers)) return "Err: " + m.identifier + " is also present in input column headers.";
    auto x = _meval(m.operation);
    _cur.add(x.first);
    _cur.var.push_back(m.identifier);
    return x.second;
  }

  result_type operator()(logicalExpr const &f) {
    auto x =  _leval(f); 
    _cur.add(x.first);
    return x.second;
  }

  result_type operator()(logicalCmd const &f) {
    auto x =  _lcmd(f); 
    return x.second;
  }

  result_type operator()(filter const &f) {
    return boost::apply_visitor(*this, f);
  }

  std::string hitReduce(ColIndices &cl) {
    if (_st == state::none) {
      _st = state::first;
      notInitial();
      return "";
    } else if (_st == state::first) {
      _st = state::many;
    }
    if (!std::includes(begin(_cur.str), end(_cur.str), begin(cl.str),
                       end(cl.str))) {
      return "The later reduce can only have same or more keys.";
    }
    return "";
  }

  bool _isInitial{true};
  void notInitial() {
    _isInitial = false;
    _meval.notInitial();
    _leval.notInitial();
    _aeval.notInitial();
    //_lcmd.notInitial();
  }
  

  result_type operator()(reduce &r) {
    std::string err;
    *_pre = _cur; // value of what pre was pointing to is changed
    std::tie(_cur, err) = _aeval(r.operation);
    if (err.size() > 0) return err;
    for (auto& it : r.cols) {
      auto colNumErr = boost::apply_visitor(colsOperand{_headers, _pre->varStr}, it);
      if (colNumErr.second.size() > 0) return colNumErr.second;
      _cur.str.push_back(colNumErr.first);
    }
    err = hitReduce(_cur);
    if (err.size() > 0) return err;
    if (_st == state::first) {
      std::copy(begin(_cur.str), end(_cur.str), back_inserter(_pre->str));
      std::copy(begin(_cur.num), end(_cur.num), back_inserter(_pre->num));
    }
    _cur.num.clear();
    _pre->uniq();
    _pre->sort();
    helper::processHeader(*_pre, _headers);
    _pre = &(r.colIndices);
    return err;
  }

  result_type operator()(zipExpr &r) {
    *_pre = _cur;
    _zipCount += 1;
    std::string err = colsEval{_global}.zipInternal(r, _zipCount);
    _zipCount = r.zipCount;
    if (err.size() > 0) return err;
    for (auto& it : r.cols) {
      auto colNumErr = boost::apply_visitor(colsOperand{_headers, _pre->varStr}, it);
      if (colNumErr.second.size() > 0) return colNumErr.second;
      _cur.str.push_back(colNumErr.first);
    }
    // std::copy(begin(r.cols), end(r.cols), std::back_inserter(_cur.str));
    // check if x.cols exist in _pre if _pre is first
    err = hitReduce(_cur);
    if (err.size() > 0) return err;
    if (_st == state::first) {
      std::copy(begin(_cur.str), end(_cur.str), back_inserter(_pre->str));
    }
    _pre->uniq();
    _pre->sort();
    helper::processHeader(*_pre, _headers);
    _pre = &(r.colIndices);
    char ch1 = (int)'a' + 2*(_zipCount - 1);
    char ch2 = ch1 + 1;
    auto nm1 = std::string{ch1};
    auto nm2 = std::string{ch2};
    std::vector<std::string> temp;
    for (auto it : _cur.num) {
      temp.push_back(std::to_string(it) + nm1);
    }
    std::move(begin(_cur.var), end(_cur.var), back_inserter(temp));
    for (auto it : r.colIndices.num) {
      temp.push_back(std::to_string(it) + nm2);
    }
    _cur.num.clear();
    _cur.var = std::move(temp);
    std::copy(begin(r.colIndices.var), end(r.colIndices.var), back_inserter(_cur.var));
    return result_type{};
  }

  result_type zipInternal(zipExpr &x, int zCount) {
    _pre = &x.first.colIndices;
    std::string inFile{x.first.fname.begin() + 1, x.first.fname.end() - 1};
    _headers = helper::headerCols(inFile);
    _meval.setHeaders(_headers);
    _leval.setHeaders(_headers);
    _aeval.setHeaders(_headers);
    _zipCount = zCount;
    x.first.index = zCount;
    for (auto& it : x.cols) {
      auto colNumErr = boost::apply_visitor(colsOperand{_headers, _pre->varStr}, it);
      if (colNumErr.second.size() > 0) return colNumErr.second;
      _cur.str.push_back(colNumErr.first);
    }
    for (auto &it : x.units) {
      auto x = boost::apply_visitor(*this, it);
      if (x.size() > 0) return x;
    }
    _cur.uniq();
    _cur.sort();
    helper::processHeader(_cur, _headers);
    *_pre = _cur;
    x.colIndices = _cur;
    x.zipCount = _zipCount;
    return "";
  }

  auto operator()(expr &x) {
    _pre = &x.first.colIndices;
    std::string inFile{x.first.fname.begin() + 1, x.first.fname.end() - 1};
    _headers = helper::headerCols(inFile);
    _meval.setHeaders(_headers);
    _leval.setHeaders(_headers);
    _aeval.setHeaders(_headers);
    x.first.index = 0;
    for (auto &it : x.units) {
      auto x = boost::apply_visitor(*this, it);
      if (x.size() > 0) return x;
    }
    _cur.uniq();
    _cur.sort();
    helper::processHeader(_cur, _headers);
    *_pre = _cur;
    x.colIndices = _cur;
    x.zipCount = _zipCount;
    auto lastCols = boost::apply_visitor(terminalCheck{_cur, _headers, _isInitial}, x.last);
    if (lastCols.second.size() > 0) return lastCols.second;
    x.colIndices.add(lastCols.first);
    return std::string{};
  }
};
/*
    ///////////////////////////////////////////////////////////////////////////
    //  The keys (string) cols evaluator
    ///////////////////////////////////////////////////////////////////////////
    struct keysEval {
    private:
      enum class state : int {
        none,
        first,
        many
      };
      state _st;
    public:
      using vectorCol = std::vector<size_t>;
      typedef vectorCol result_type;

      result_type operator()(map const &m) {
        return result_type{};
      }

      result_type operator()(filter const &f) {
        return result_type{};
      }

      result_type operator()(reduce const &r) {
        _st = (_st == state::none) ? state::first : state::many;
        return r.cols;
      }

      auto operator()(expr const& x) {
        std::pair<result_type, std::string> res{};
        _st = state::none;
        for (const auto &it : x.units) {
          auto x = boost::apply_visitor(*this, it);
          if (_st == state::first) {
            res.first = x;
          } else if (_st == state::many && !std::includes(
                begin(res.first), end(res.first), begin(x), end(x))) {
            return std::make_pair(result_type{}, std::string{"The later reduce
   can only group by subset of prior ones."});
          }
        }
        return res;
      }
    };
*/
} // namespace ast
} // namespace pawn
} // namespace client

BOOST_FUSION_ADAPT_STRUCT(client::pawn::ast::fileName, (std::string, name))

BOOST_FUSION_ADAPT_STRUCT(client::pawn::ast::src,
                          (std::string, fname)
                          /*(client::helper::ColIndices, colIndices)*/)

BOOST_FUSION_ADAPT_STRUCT(client::pawn::ast::map,
                          (std::string, identifier)
                          (client::math::ast::expr, operation))

BOOST_FUSION_ADAPT_STRUCT(client::pawn::ast::reduce,
                          (std::vector<client::pawn::ast::strOperand>, cols)
                          (client::reduce::ast::expr, operation)
                          /*(client::helper::ColIndices, colIndices)*/)

BOOST_FUSION_ADAPT_STRUCT(client::pawn::ast::zipExpr,
                          (std::vector<client::pawn::ast::strOperand>, cols)
                          (client::pawn::ast::src, first)
                          (std::list<client::pawn::ast::unit>, units)
                          /*(client::helper::ColIndices, colIndices)*/)

BOOST_FUSION_ADAPT_STRUCT(client::pawn::ast::expr,
                          (client::pawn::ast::src, first)
                          (std::list<client::pawn::ast::unit>, units)
                          (client::pawn::ast::terminal, last))

BOOST_FUSION_ADAPT_STRUCT(client::pawn::ast::saveStr,
                          (client::pawn::ast::strOperand, src)
                          (std::string, dest))

BOOST_FUSION_ADAPT_STRUCT(client::pawn::ast::saveNum,
                          (client::pawn::ast::numSrc, src)(std::string, dest))

BOOST_FUSION_ADAPT_STRUCT(client::pawn::ast::queryName, (std::string, name))

#endif
