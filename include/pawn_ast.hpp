/*=============================================================================
    Copyright (c) 2001-2011 Joel de Guzman

    Distributed under the Boost Software License, Version 1.0. (See accompanying
    file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
=============================================================================*/
#if !defined(PAWN_AST_HPP)
#define PAWN_AST_HPP

#include <boost/config/warning_disable.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/fusion/include/adapt_struct.hpp>
#include <boost/fusion/include/io.hpp>
#include <boost/optional.hpp>
#include <list>
#include <map>

#include <mast.hpp>
#include <last.hpp>
#include <helper.hpp>

namespace client { namespace pawn { namespace ast
{
    using mathExpr = client::math::ast::expr;
    using logicalExpr = client::logical::ast::expr;

    using src = std::string;

    //using map = std::string;
    struct map {
      std::string identifier;
      mathExpr operation;
    };

    using filter = logicalExpr;

    typedef boost::variant<map , filter> unit;

    struct expr
    {
        src first;
        std::list<unit> units;
        //terminal last;
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

        void operator()(expr const& x) const {
          std::cout << "file " << x.first << " | ";
          for (const auto &it : x.units) {
            boost::apply_visitor(*this, it);
          }
        }
    };

    ///////////////////////////////////////////////////////////////////////////
    //  The AST evaluator
    ///////////////////////////////////////////////////////////////////////////
    struct colsEval {
    private:
      using vectorCol = std::vector<size_t>;
      using vectorVar = std::vector<std::string>;

      vectorVar _v;
      client::math::ast::colsEval _meval;
      client::logical::ast::colsEval _leval;
    public:
      typedef std::pair<vectorCol, std::string> result_type;
      colsEval() : _v{}, _meval{_v}, _leval{_v} {}

      const vectorVar& variables() const { return _v; }

      result_type operator()(map const &m) {
        auto x = _meval(m.operation);
        _v.push_back(m.identifier);
        return x;
      }

      result_type operator()(filter const &f) {
        return _leval(f);
      }

      result_type operator()(expr const& x) {
        result_type res{};
        for (const auto &it : x.units) {
          auto x = boost::apply_visitor(*this, it);
          if (x.second.size() > 0) return x;
          std::move(begin(x.first), end(x.first), back_inserter(res.first));
        }
        return res;
      }
    };
}}}

BOOST_FUSION_ADAPT_STRUCT(
    client::pawn::ast::map,
    (std::string, identifier)
    (client::math::ast::expr, operation)
)

BOOST_FUSION_ADAPT_STRUCT(
    client::pawn::ast::expr,
    (std::string, first)
    (std::list<client::pawn::ast::unit>, units)
)
#endif
