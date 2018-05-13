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
      using vectorCol = std::vector<size_t>;
      using vectorVar = std::vector<std::string>;
      using ColIndices = client::helper::ColIndices;
      const vectorVar &_v;
    public:
        using result_type = std::pair<ColIndices, std::string>;
        colsEval(const vectorVar &v) : _v{v} {}
        result_type operator()(variable const &x) const { 
          auto it = std::find(begin(_v), end(_v), x);
          if (it == std::end(_v)) return std::make_pair(ColIndices{}, x);
          return result_type{};
        }
        result_type operator()(column const &x) const { 
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
    public:
        using retFnT = std::function<std::string(const std::vector<std::string>&)>;
        typedef retFnT result_type;

        evaluator(helper::positionTeller p) : _index{p} {}
        retFnT operator()(quoted n) const { return whatever{n.val}; }
        retFnT operator()(variable const &x) const { 
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
