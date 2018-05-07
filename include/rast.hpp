/*=============================================================================
    Copyright (c) 2001-2011 Joel de Guzman

    Distributed under the Boost Software License, Version 1.0. (See accompanying
    file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
=============================================================================*/
#if !defined(PAWN_RAST_HPP)
#define PAWN_RAST_HPP

#include <boost/config/warning_disable.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/fusion/include/adapt_struct.hpp>
#include <boost/fusion/include/io.hpp>
#include <boost/optional.hpp>
#include <list>
#include <map>

#include "mast.hpp"

namespace client { namespace relational { namespace ast
{
    enum class optoken : int
    {
        equal,
        not_equal,
        less,
        less_equal,
        greater,
        greater_equal
    };

    using operand = client::math::ast::expr;

    struct expr
    {
        operand lhs;
        optoken operator_;
        operand rhs;
    };

    struct printer {
        typedef void result_type;

        void operator()(optoken const &o) const {
            switch (o)
            {
                case optoken::equal: std::cout << " eq"; break;
                case optoken::not_equal: std::cout << " neq"; break;
                case optoken::less: std::cout << " lt"; break;
                case optoken::less_equal: std::cout << " leq"; break;
                case optoken::greater: std::cout << " gt"; break;
                case optoken::greater_equal: std::cout << " geq"; break;
            }
        }

        void operator()(expr const& x) const {
            client::math::ast::printer print;
            print(x.lhs);
            (*this)(x.operator_);
            std::cout << ' ';
            print(x.rhs);
        }
    };

    struct colsEval {
    private:
      using vectorCol = std::vector<size_t>;
      using vectorVar = std::vector<std::string>;
      const vectorVar &_v;
    public:
        using result_type = std::pair<vectorCol, std::string>;
        colsEval(const vectorVar &v) : _v{v} {}
        result_type operator()(expr const& x) const {
            client::math::ast::colsEval mColsEval(_v);
            auto res = mColsEval(x.lhs);
            if (res.second.size() > 0) return res;
            auto y = mColsEval(x.rhs);
            if (y.second.size() > 0) return y;
            std::move(begin(y.first), end(y.first), back_inserter(res.first));
            return res;
        }
    };

    ///////////////////////////////////////////////////////////////////////////
    //  The AST evaluator
    ///////////////////////////////////////////////////////////////////////////
    struct evaluator {
    private:
        client::math::ast::evaluator _meval;
    public:
        using mretFnT = client::math::ast::evaluator::retFnT;
        using retFnT = std::function<bool(const std::vector<double>&)>;
        evaluator(client::math::ast::evaluator meval) : _meval{meval} {}
        typedef retFnT result_type;

        retFnT operator()(optoken const &o, mretFnT const &lhs, mretFnT const &rhs) const {
            switch (o)
            {
                case optoken::equal: return [lhs, rhs](const std::vector<double> &v) { return lhs(v) == rhs(v); };
                case optoken::not_equal: return [lhs, rhs](const std::vector<double> &v) { return lhs(v) != rhs(v); };
                case optoken::less: return [lhs, rhs](const std::vector<double> &v) { return lhs(v) < rhs(v); };
                case optoken::less_equal: return [lhs, rhs](const std::vector<double> &v) { return lhs(v) <= rhs(v); };
                case optoken::greater: return [lhs, rhs](const std::vector<double> &v) { return lhs(v) > rhs(v); };
                case optoken::greater_equal: return [lhs, rhs](const std::vector<double> &v) { return lhs(v) >= rhs(v); };
                default: BOOST_ASSERT(0); return rhs;
            }
            BOOST_ASSERT(0);
            return rhs;
        }

        retFnT operator()(expr const& x) const
        {
           return (*this)(x.operator_, _meval(x.lhs), _meval(x.rhs)); 
        }
    };
}}}

BOOST_FUSION_ADAPT_STRUCT(
    client::relational::ast::expr,
    (client::math::ast::expr, lhs)
    (client::relational::ast::optoken, operator_)
    (client::math::ast::expr, rhs)
)
#endif
