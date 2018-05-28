/*=============================================================================
    Copyright (c) 2001-2011 Joel de Guzman

    Distributed under the Boost Software License, Version 1.0. (See accompanying
    file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
=============================================================================*/
#if !defined(PAWN_AAST_HPP)
#define PAWN_AAST_HPP

#include <boost/config/warning_disable.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/fusion/include/adapt_struct.hpp>
#include <boost/fusion/include/io.hpp>
#include <boost/optional.hpp>

#include <iostream>
#include <list>
#include <map>
#include <vector>

#include <helper.hpp>

namespace client { namespace reduce { namespace ast
{
    using variable = std::string;
    using column = unsigned int;

    enum class optoken : int {
      sum,
      max
    };

    typedef boost::variant<variable , column> operand;

    struct operation {
      optoken operator_;
      operand operand_;
    };

    using expr = std::list<operation>;

    // print functions for debugging
    struct printer {
        typedef void result_type;

        void operator()(variable const& x) const { std::cout << '%' << x; }
        void operator()(column const& x) const { std::cout << '$' << x; }
        void operator()(optoken const &o) const {
            switch (o) {
                case optoken::sum: std::cout << " sum"; break;
                case optoken::max: std::cout << " max"; break;
            }
        }
        
        void operator()(operation const &x) const {
          (*this)(x.operator_);
          boost::apply_visitor(*this, x.operand_);
        }

        void operator()(expr const& x) const {
          for (auto&it : x) (*this)(it);
        }
    };

    struct colsEval {
    private:
      using ColIndices = client::helper::ColIndices;
      const ColIndices &_pre;
      bool _isInitial {true};
      std::vector<std::string> _headers;
    public:
      using result_type = std::pair<ColIndices, std::string>;
      colsEval(const ColIndices &v) : _pre{v} {}
      void setHeaders(const std::vector<std::string>& h) { _headers = h; }
      void notInitial() { _isInitial = false; }
      result_type operator()(variable const &x) { 
        auto jt = std::find(begin(_headers), end(_headers), x);
        if (jt != std::end(_headers)) return (*this)((unsigned int)(jt - std::begin(_headers)) + 1, x);
        auto it = std::find(begin(_pre.var), end(_pre.var), x);
        if (it == std::end(_pre.var)) return std::make_pair(ColIndices{}, "Error: " + x + " used before declaration.");
        ColIndices res;
        res.var.push_back(_nm + "_" + x);
        return std::make_pair(res, "");
      }
      result_type operator()(column const &x, std::string colName = "") { 
        if (!_isInitial) return std::make_pair(ColIndices{}, "Can't access number columns via column index after reduce.");
        ColIndices res;
        res.num.push_back(x);
        res.var.push_back(_nm + "_" + ((colName.size() > 0) ? colName : std::to_string(x)));
        return std::make_pair(res, "");
      }
      std::string _nm;
      result_type operator()(operation const& x) {
          switch (x.operator_) {
              case optoken::sum: _nm = std::string{"sum"}; break;
              case optoken::max: _nm = std::string{"max"};
          }
          return boost::apply_visitor(*this, x.operand_);
      }
      result_type operator()(expr const& e) {
          result_type res{};
          for (const auto& oper : e) {
            auto x = (*this)(oper);
            if (x.second.size() > 0) return x;
            res.first.add(x.first);
          }
          return res;
      }
  };

    ///////////////////////////////////////////////////////////////////////////
    //  The AST evaluator
    ///////////////////////////////////////////////////////////////////////////
    struct evaluator {
    private:
      struct indexHelper {
        const helper::positionTeller _index;
        indexHelper(helper::positionTeller p) : _index{p} {}
        int operator()(variable var) const {
          return _index.var(var);
        }

        int operator()(column n) const {
          return _index.num(n);
        }
      };

        const indexHelper _index;
        bool _sameIndex {false};
    public:
        using resT = std::tuple<std::vector<double>>&;
        using keyT = const std::vector<std::string>&;
        using rowT = const std::vector<double>&;
        using retFnT = std::function<resT(resT, keyT, rowT)>;
        typedef retFnT result_type;
        void sameIndex(bool isSameIndex = true) { _sameIndex = isSameIndex; }

        evaluator(helper::positionTeller p) : _index{p} {}

        retFnT operator()(operation const& x, int i) const {
            int j = _sameIndex ? i : boost::apply_visitor(_index, x.operand_);
            switch (x.operator_)
            {
                case optoken::sum: return [i, j](resT r, keyT, rowT c) -> auto& { std::get<0>(r)[i] += c[j]; return r; };
                case optoken::max: return [i, j](resT r, keyT, rowT c) -> auto& { if (c[j] > std::get<0>(r)[i]) std::get<0>(r)[i] = c[j]; return r; };
            }
            return [i, j](resT r, keyT, rowT c) -> auto& { if (c[j] > std::get<0>(r)[i]) std::get<0>(r)[i] = c[j]; return r; };
        }

        std::vector<retFnT> operator()(expr const& x) const {
            std::vector<retFnT> v;
            int i = 0;
            for (const auto& oper : x) {
                v.push_back((*this)(oper, i++));
            }
            return v;
        }

    };
}}}

BOOST_FUSION_ADAPT_STRUCT(
    client::reduce::ast::operation,
    (client::reduce::ast::optoken, operator_)
    (client::reduce::ast::operand, operand_)
)
#endif
