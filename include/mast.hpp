/*=============================================================================
    Copyright (c) 2001-2011 Joel de Guzman

    Distributed under the Boost Software License, Version 1.0. (See accompanying
    file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
=============================================================================*/
#if !defined(PAWN_MAST_HPP)
#define PAWN_MAST_HPP

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

namespace client { namespace math { namespace ast
{
    struct nil {};
    struct unary;
    struct expr;

    using variable = std::string;
    using column = unsigned int;

    typedef boost::variant<
            nil
          , double
          , variable
          , column
          , boost::recursive_wrapper<unary>
          , boost::recursive_wrapper<expr>
        >
    operand;

    enum class optoken : int
    {
        plus,
        minus,
        times,
        divide,
        positive,
        negative
    };

    struct unary
    {
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

    // print functions for debugging
    inline std::ostream& operator<<(std::ostream& out, nil) { out << "nil"; return out; }
    //inline std::ostream& operator<<(std::ostream& out, variable const& var) { out << var.name; return out; }

    struct printer {
        typedef void result_type;

        void operator()(nil) const {std::cout << '_'; }
        void operator()(double n) const { std::cout << n; }
        void operator()(variable const& x) const { std::cout << '%' << x; }
        void operator()(column const& x) const { std::cout << '$' << x; }

        void operator()(optoken const &o) const {
            switch (o)
            {
                case optoken::plus: std::cout << " add"; break;
                case optoken::minus: std::cout << " subt"; break;
                case optoken::times: std::cout << " mult"; break;
                case optoken::divide: std::cout << " div"; break;
                case optoken::positive: std::cout << " pos"; break;
                case optoken::negative: std::cout << " neg"; break;
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
      const ColIndices &_pre;
      bool _isInitial {true};
    public:
        using result_type = std::pair<ColIndices, std::string>;
        colsEval(const ColIndices &v) : _pre{v} {}
        void notInitial() { _isInitial = false; }
        result_type operator()(nil) const { BOOST_ASSERT(0); return result_type{}; }
        result_type operator()(double n) const { return result_type{}; }
        result_type operator()(variable const &x) const {
          auto it = std::find(begin(_pre.var), end(_pre.var), x);
          if (it == std::end(_pre.var)) return std::make_pair(ColIndices{}, "Error: " + x + " used before declaration.");
          return result_type{};
        }
        result_type operator()(column const &x) const {
          if (!_isInitial) return std::make_pair(ColIndices{}, "Can't access number columns via column index after reduce.");
          ColIndices res;
          res.num.push_back(x);
          return std::make_pair(res, "");
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
      whatever(double x) : _x{x} {}
      auto operator() (const std::vector<double>&) { return _x; }
    private:
      double _x;
    };

    struct evaluator {
    private:
        const helper::positionTeller _index;
    public:
        using retFnT = std::function<double(const std::vector<double>&)>;
        typedef retFnT result_type;

        evaluator(helper::positionTeller p) : _index{p} {}
        retFnT operator()(nil) const { BOOST_ASSERT(0); return whatever{0.0}; }
        retFnT operator()(double n) const { return whatever{n}; }
        retFnT operator()(variable const &x) const { 
          auto y = _index.var(x);
          return [y](const std::vector<double> &v) { return v[y]; };
        }
        retFnT operator()(column const &x) const { 
          auto y = _index.num(x);
          return [y](const std::vector<double> &v) { return v[y]; };
        }

        retFnT operator()(optoken const &o, retFnT const &lhs, retFnT const &rhs) const {
            switch (o)
            {
                case optoken::plus: return [lhs, rhs](const std::vector<double> &v) { return lhs(v) + rhs(v); };
                case optoken::minus: return [lhs, rhs](const std::vector<double> &v) { return lhs(v) - rhs(v); };
                case optoken::times: return [lhs, rhs](const std::vector<double> &v) { return lhs(v) * rhs(v); };
                case optoken::divide: return [lhs, rhs](const std::vector<double> &v) { return lhs(v) / rhs(v); };
                default: BOOST_ASSERT(0); return rhs;
            }
            BOOST_ASSERT(0);
            return rhs;
        }

        retFnT operator()(optoken const &o, retFnT const &rhs) const {
            switch (o)
            {
                case optoken::positive: return [rhs](const std::vector<double> &v) { return rhs(v); };
                case optoken::negative:  return [rhs](const std::vector<double> &v) { return - rhs(v); };
                default: BOOST_ASSERT(0); return rhs;
            }
            BOOST_ASSERT(0);
            return rhs;
        }

        retFnT operator()(unary const& x) const {
            retFnT rhs = boost::apply_visitor(*this, x.operand_);
            return (*this)(x.operator_, rhs);
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
    client::math::ast::unary,
    (client::math::ast::optoken, operator_)
    (client::math::ast::operand, operand_)
)

BOOST_FUSION_ADAPT_STRUCT(
    client::math::ast::operation,
    (client::math::ast::optoken, operator_)
    (client::math::ast::operand, operand_)
)

BOOST_FUSION_ADAPT_STRUCT(
    client::math::ast::expr,
    (client::math::ast::operand, first)
    (std::list<client::math::ast::operation>, rest)
)
#endif
