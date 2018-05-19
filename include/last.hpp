/*=============================================================================
    Copyright (c) 2001-2011 Joel de Guzman

    Distributed under the Boost Software License, Version 1.0. (See accompanying
    file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
=============================================================================*/
#if !defined(PAWN_LAST_HPP)
#define PAWN_LAST_HPP

#include <boost/config/warning_disable.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/fusion/include/adapt_struct.hpp>
#include <boost/fusion/include/io.hpp>
#include <boost/optional.hpp>
#include <list>
#include <map>

#include "rast.hpp"

namespace client { namespace logical { namespace ast
{
    struct unary;
    struct expr;

    typedef boost::variant<
          bool
          , boost::recursive_wrapper<unary>
          , client::relational::ast::expr
          , boost::recursive_wrapper<expr>
        >
    operand;

    enum class optoken : int {
        conjunct,
        disjunct,
        negate
    };

    struct unary {
        optoken operator_;
        operand operand_;
    };

    struct operation
    {
        optoken operator_;
        operand operand_;
    };

    struct expr
    {
        operand first;
        std::list<operation> rest;
    };

    struct printer {
    private:
        client::relational::ast::printer print;
    public:
        typedef void result_type;

        void operator()(bool n) const { std::cout << std::boolalpha << n; }


        void operator()(client::relational::ast::expr const& x) const
        {
            print(x);
        }

        void operator()(optoken const &o) const {
            switch (o)
            {
                case optoken::conjunct: std::cout << " and"; break;
                case optoken::disjunct: std::cout << " or"; break;
                case optoken::negate: std::cout << " not"; break;
            }
        }

        void operator()(unary const& x) const
        {
            boost::apply_visitor(*this, x.operand_);
            (*this)(x.operator_);
        }

        void operator()(operation const& x) const
        {
            boost::apply_visitor(*this, x.operand_);
            (*this)(x.operator_);
        }

        void operator()(expr const& x) const
        {
            boost::apply_visitor(*this, x.first);
            for (const auto& oper : x.rest) {
                std::cout << ' ';
                (*this)(oper);
            }
        }
    };

    struct colsEval {
    private:
        using ColIndices = client::helper::ColIndices;

        client::relational::ast::colsEval rColsEval;
    public:
        using result_type = std::pair<ColIndices, std::string>;
        colsEval(const ColIndices &v, const helper::Global &g) : rColsEval{v, g} {}

        void notInitial() { rColsEval.notInitial(); }
        result_type operator()(bool n) const { return result_type{}; }

        result_type operator()(client::relational::ast::expr const& x) const {
          return rColsEval(x);
        }

        result_type operator()(unary const& x) const {
          return boost::apply_visitor(*this, x.operand_);
        }

        result_type operator()(operation const& x) const {
          return boost::apply_visitor(*this, x.operand_);
        }

        result_type operator()(expr const& e) const {
            result_type res{};
            auto x = boost::apply_visitor(*this, e.first);
            if (x.second.size() > 0) return x;
            res.first.add(x.first);
            for (const auto& oper : e.rest) {
              x = (*this)(oper);
              if (x.second.size() > 0) return x;
              res.first.add(x.first);
            }
            return res;
        }
    };



    ///////////////////////////////////////////////////////////////////////////
    //  The AST evaluator
    ///////////////////////////////////////////////////////////////////////////
    struct whatever {
      whatever(bool x) : _x{x} {}
      auto operator() (const std::vector<std::string>&, const std::vector<double>&) { return _x; }
    private:
      bool _x;
    };

    struct evaluator
    {
    private:
        client::relational::ast::evaluator _reval;
    public:
        using retFnT = std::function<bool(const std::vector<std::string>&, const std::vector<double>&)>;
        evaluator(client::relational::ast::evaluator reval) : _reval{reval} {}
        evaluator(helper::positionTeller p, const helper::Global &g) : _reval{p, g} {}
        typedef retFnT result_type;

        retFnT operator()(bool n) const { return whatever{n}; }
        retFnT operator()(client::relational::ast::expr const& x) const { 
          return _reval(x);
        }

        retFnT operator()(optoken const &o, retFnT const &lhs, retFnT const &rhs) const {
            switch (o)
            {
                case optoken::conjunct: return [lhs, rhs](const std::vector<std::string>& v1, const std::vector<double> &v2) { return lhs(v1, v2) && rhs(v1, v2); };
                case optoken::disjunct: return [lhs, rhs](const std::vector<std::string>& v1, const std::vector<double> &v2) { return lhs(v1, v2) || rhs(v1, v2); };
                default: BOOST_ASSERT(0); return rhs;
            }
            BOOST_ASSERT(0);
            return rhs;
        }

        retFnT operator()(unary const& x) const
        {
            retFnT rhs = boost::apply_visitor(*this, x.operand_);
            return [rhs](const std::vector<std::string>& v1, const std::vector<double> &v2) { return !rhs(v1, v2); };
        }

        retFnT operator()(operation const& x, retFnT const& lhs) const
        {
            retFnT rhs = boost::apply_visitor(*this, x.operand_);
            return (*this)(x.operator_, lhs, rhs);
        }
        retFnT operator()(expr const& x) const
        {
            retFnT state = boost::apply_visitor(*this, x.first);
            for (const auto& oper : x.rest) {
                state = (*this)(oper, state);
            }
            return state;
        }
    };
}}}

BOOST_FUSION_ADAPT_STRUCT(
    client::logical::ast::unary,
    (client::logical::ast::optoken, operator_)
    (client::logical::ast::operand, operand_)
)

BOOST_FUSION_ADAPT_STRUCT(
    client::logical::ast::operation,
    (client::logical::ast::optoken, operator_)
    (client::logical::ast::operand, operand_)
)

BOOST_FUSION_ADAPT_STRUCT(
    client::logical::ast::expr,
    (client::logical::ast::operand, first)
    (std::list<client::logical::ast::operation>, rest)
)
#endif
