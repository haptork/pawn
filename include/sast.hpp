/*=============================================================================
    Copyright (c) 2001-2011 Joel de Guzman

    Distributed under the Boost Software License, Version 1.0. (See accompanying
    file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
=============================================================================*/
#if !defined(PAWN_SAST_HPP)
#define PAWN_SAST_HPP

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

namespace client { namespace str { namespace ast
{
    using variable = std::string;
    using column = unsigned int;
    struct quoted {
      std::string val; 
    };
    typedef boost::variant<variable, column, quoted> expr;

    struct printer {
        typedef void result_type;

        void operator()(variable const& x) const { std::cout << '%' << x; }
        void operator()(column const& x) const { std::cout << '%' << x; }
        void operator()(quoted const& x) const { std::cout << '"' << x.val << '"'; }

        void operator()(expr const& x) const {
          boost::apply_visitor(*this, x);
        }
    };

    struct colsEval {
    private:
      using ColIndices = client::helper::ColIndices;
      using Global = client::helper::Global;
      const ColIndices &_pre;
      const Global &_global;
      bool _isInitial {true};
    public:
      using result_type = std::pair<ColIndices, std::string>;
      colsEval(const ColIndices &v, const Global &global) : _pre{v}, _global{global} {}
      void notInitial() { _isInitial = false; }
      result_type operator()(variable const &x) const { 
        if (_global.gVarsS.find(x) != std::end(_global.gVarsS)) return result_type{};
        auto it = std::find(begin(_pre.var), end(_pre.var), x);
        if (it == std::end(_pre.var)) return std::make_pair(ColIndices{}, "Error: " + x + " used before declaration.");
        return result_type{};
      }
      result_type operator()(column const &x) const { 
        if (!_isInitial) {
          auto it = std::find(begin(_pre.str), end(_pre.str), x);
          if (it == std::end(_pre.str)) {
            return std::make_pair(ColIndices{}, "Error: string column " + std::to_string(x) + " does not persist after reduction.");
          }
          return result_type{};
        }
        ColIndices res;
        res.str.push_back(x);
        return std::make_pair(res, "");
      }
      result_type operator()(quoted const &x) const { 
        return result_type{};
      }
      result_type operator()(expr const& e) const {
        return boost::apply_visitor(*this, e);
      }
    };

    ///////////////////////////////////////////////////////////////////////////
    //  The AST evaluator
    ///////////////////////////////////////////////////////////////////////////
    struct whatever {
      whatever(std::string x) : _x{x} {}
      auto operator() (const std::vector<std::string>&) { return _x; }
    private:
      std::string _x;
    };

    struct evaluator {
    private:
        const helper::positionTeller _index;
        const helper::Global &_global;
    public:
        using retFnT = std::function<std::string(const std::vector<std::string>&)>;
        typedef retFnT result_type;

        evaluator(helper::positionTeller p, const helper::Global &g) : _index{p}, _global{g} {}
        retFnT operator()(quoted n) const { return whatever{n.val}; }
        retFnT operator()(variable const &x) const { 
          auto it = _global.gVarsS.find(x);
          if (it != std::end(_global.gVarsS)) {
            auto y = it->second;
            return [y](const std::vector<std::string> &v) { return y; };
          }
          auto y = _index.var(x);
          return [y](const std::vector<std::string> &v) { return v[y]; };
        }
        retFnT operator()(column const &x) const { 
          auto y = _index.str(x);
          return [y](const std::vector<std::string> &v) { return v[y]; };
        }

        retFnT operator()(expr const& x) const {
          return boost::apply_visitor(*this, x);
        }
    };
}}}

BOOST_FUSION_ADAPT_STRUCT(
    client::str::ast::quoted,
    (std::string, val)
)
#endif
