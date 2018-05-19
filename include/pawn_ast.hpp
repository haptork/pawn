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
#include <mast.hpp>

namespace client {
namespace pawn {
namespace ast {
using ColIndices = client::helper::ColIndices;
using uint_ = unsigned int;
using mathExpr = client::math::ast::expr;
using logicalExpr = client::logical::ast::expr;
using reduceExpr = client::reduce::ast::expr;

struct src {
  std::string fname;
  ColIndices colIndices;
};

using quoted_stringT = std::string;

using identifierT = std::string;
using strIdentifierT = std::string;

struct map {
  identifierT identifier;
  mathExpr operation;
};

using filter = logicalExpr;

struct reduce {
  std::vector<uint_> cols;
  reduceExpr operation;
  ColIndices colIndices;
};

struct zipExpr;

using unit =
    boost::variant<map, filter, reduce, boost::recursive_wrapper<zipExpr>>;

struct zipExpr {
  std::vector<uint_> cols;
  src first;
  std::list<unit> units;
  ColIndices colIndices;
};

using numSrc = boost::variant<identifierT, uint_>;

struct saveNum {
  numSrc src;
  identifierT dest;
};

struct saveStr {
  uint_ src;
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
};

struct printer {
  typedef void result_type;

  void operator()(map const &m) const {
    client::math::ast::printer mathPrint;
    std::cout << "map " << m.identifier << " to ";
    mathPrint(m.operation);
    std::cout << " | ";
  }

  void operator()(filter const &f) const {
    client::logical::ast::printer logicalPrint;
    std::cout << "filter with ";
    logicalPrint(f);
    std::cout << " | ";
  }

  void operator()(zipExpr const &z) const {
    std::cout << "zip ";
    if (!z.cols.empty()) {
      std::cout << "group by ";
      for (auto it : z.cols) {
        std::cout << it << ", ";
      }
    }
    std::cout << "(";
    std::cout << "file " << z.first.fname << " | ";
    for (const auto &it : z.units) {
      boost::apply_visitor(*this, it);
    }
    std::cout << ")";
  }

  void operator()(reduce const &r) const {
    client::reduce::ast::printer reducePrint;
    std::cout << "reduce to ";
    reducePrint(r.operation);
    if (!r.cols.empty()) {
      std::cout << " group by ";
      for (auto it : r.cols) {
        std::cout << it << ", ";
      }
    }
    client::helper::print(r.colIndices);
    std::cout << " | ";
  }

  void operator()(const saveNum &s) const {
    std::cout << " saveNum to " << s.dest;
  }

  void operator()(const saveStr &s) const {
    std::cout << " saveStr from " << s.src << " to " << s.dest;
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
    std::cout << "file " << x.first.fname << " | ";
    client::helper::print(x.first.colIndices);
    for (const auto &it : x.units) {
      boost::apply_visitor(*this, it);
    }
    boost::apply_visitor(*this, x.last);
  }
};

///////////////////////////////////////////////////////////////////////////
//  The numerical cols evaluator
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
  enum class state : int { none, first, many };
  state _st{state::none};

public:
  typedef std::string result_type;
  colsEval(const Global &global)
      : _global{global}, _cur{}, _meval{_cur, _global},
        _leval{_cur, _global}, _aeval{_cur} {}

  result_type operator()(map const &m) {
    auto i = std::find(std::begin(_cur.var), std::end(_cur.var), m.identifier);
    if (i != std::end(_cur.var)) return "Err: " + m.identifier + " redeclared.";
    auto x = _meval(m.operation);
    _cur.add(x.first);
    _cur.var.push_back(m.identifier);
    return x.second;
  }

  result_type operator()(filter const &f) {
    auto x =  _leval(f); 
    _cur.add(x.first);
    return x.second;
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
  }

  result_type operator()(reduce &r) {
    std::string err;
    *_pre = _cur;
    std::tie(_cur, err) = _aeval(r.operation);
    if (err.size() > 0) return err;
    std::copy(begin(r.cols), end(r.cols), std::back_inserter(_cur.str));
    err = hitReduce(_cur);
    if (err.size() > 0) return err;
    if (_st == state::first) {
      std::copy(begin(_cur.str), end(_cur.str), back_inserter(_pre->str));
    }
    _pre->uniq();
    _pre->sort();
    _pre = &(r.colIndices);
    return err;
  }

  result_type operator()(zipExpr &z) {
    /*
        std::pair<std::vector<ColIndices>, std::string> res;
        res.first.emplace_back();
        for (const auto &it : x.units) {
          auto x = boost::apply_visitor(*this, it);
          if (x.second.size() > 0) {
            res.second = x.second;
            return res;
          }
            if (_st == state::first) {
              std::copy(begin(x.first.str), end(x.first.str),
       back_inserter(res.first[0].str));
            }
            res.first.push_back(x.first);
          } else {
            res.first[res.first.size() - 1].add(x.first);
          }
          _cur = res.first[res.first.size() - 1];
        }
        res.second = boost::apply_visitor(terminalCheck{_cur}, x.last);
        return res;
        */
    return result_type{};
  }

  struct terminalCheck {
  private:
    ColIndices &_pre;

  public:
    using result_type = std::string;
    terminalCheck(ColIndices &cols) : _pre{cols} {}
    result_type operator()(const queryName &) const { return ""; }
    result_type operator()(const fileName &) const { return ""; }
    result_type operator()(const saveVal &s) const {
      for (auto &it : s) {
        auto x = boost::apply_visitor(*this, it);
        if (x.size() > 0) return x;
      }
      return "";
    }
    result_type operator()(identifierT s) const {
      auto it = std::find(begin(_pre.var), end(_pre.var), s);
      if (it == std::end(_pre.var))
        return "Error: $" + s + " used before declaration.";
      return "";
    }
    result_type operator()(uint_ s) const {
      auto it = std::find(begin(_pre.num), end(_pre.num), s);
      if (it == std::end(_pre.num))
        return "Error: $" + std::to_string(s) + " used before declaration.";
      return "";
    }
    result_type operator()(const saveNum &s) const {
      return boost::apply_visitor(*this, s.src);
    }

    result_type operator()(const saveStr &s) const {
      auto it = std::find(begin(_pre.str), end(_pre.str), s.src);
      if (it == std::end(_pre.str))
        return "Error: %" + std::to_string(s.src) + " used before declaration.";
      return "";
    }
  };

  auto operator()(expr &x) {
    _pre = &x.first.colIndices;
    for (auto &it : x.units) {
      auto x = boost::apply_visitor(*this, it);
      if (x.size() > 0) return x;
    }
    *_pre = _cur;
    x.colIndices = _cur;
    return boost::apply_visitor(terminalCheck{_cur}, x.last);
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
                          (std::vector<client::pawn::ast::uint_>, cols)
                          (client::reduce::ast::expr, operation)
                          /*(client::helper::ColIndices, colIndices)*/)

BOOST_FUSION_ADAPT_STRUCT(client::pawn::ast::zipExpr,
                          (std::vector<client::pawn::ast::uint_>, cols)
                          (client::pawn::ast::src, first)
                          (std::list<client::pawn::ast::unit>, units)
                          /*(client::helper::ColIndices, colIndices)*/)

BOOST_FUSION_ADAPT_STRUCT(client::pawn::ast::expr,
                          (client::pawn::ast::src, first)
                          (std::list<client::pawn::ast::unit>, units)
                          (client::pawn::ast::terminal, last))

BOOST_FUSION_ADAPT_STRUCT(client::pawn::ast::saveStr,
                          (unsigned int, src)(std::string, dest))

BOOST_FUSION_ADAPT_STRUCT(client::pawn::ast::saveNum,
                          (client::pawn::ast::numSrc, src)(std::string, dest))

BOOST_FUSION_ADAPT_STRUCT(client::pawn::ast::queryName, (std::string, name))

#endif
