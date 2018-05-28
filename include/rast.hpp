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
#include "sast.hpp"

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

    using mathOperand = client::math::ast::expr;
    using strOperand = client::str::ast::expr;

    struct mathOp {
        mathOperand lhs;
        optoken operator_;
        mathOperand rhs;
    };

    struct strOp {
        strOperand lhs;
        optoken operator_;
        strOperand rhs;
    };

    using expr = boost::variant<mathOp, strOp>;

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

        void operator()(mathOp const& x) const {
            client::math::ast::printer print;
            print(x.lhs);
            (*this)(x.operator_);
            std::cout << ' ';
            print(x.rhs);
        }

        void operator()(strOp const& x) const {
            client::str::ast::printer print;
            print(x.lhs);
            (*this)(x.operator_);
            std::cout << ' ';
            print(x.rhs);
        }

        void operator()(expr const& x) const {
          boost::apply_visitor(*this, x);
        }
    };

    struct colsEval {
    private:
      using ColIndices = client::helper::ColIndices;
      using Global = client::helper::Global;
      const ColIndices &_v;
      const Global &_global;
      bool _isInitial {true};
      std::vector<std::string> _headers;
    public:
        using result_type = std::pair<ColIndices, std::string>;
        colsEval(const ColIndices &v, const Global &global) : _v{v}, _global{global} {}
        void setHeaders(const std::vector<std::string>& h) { _headers = h; }
        void notInitial() { _isInitial = false; }
        result_type operator()(mathOp const& x) const {
            client::math::ast::colsEval mColsEval(_v, _global);
            mColsEval.setHeaders(_headers);
            if (!_isInitial) mColsEval.notInitial();
            auto res = mColsEval(x.lhs);
            if (res.second.size() > 0) return res;
            auto y = mColsEval(x.rhs);
            if (y.second.size() > 0) return y;
            res.first.add(y.first);
            return res;
        }
        result_type operator()(strOp const& x) const {
            client::str::ast::colsEval mColsEval(_v, _global);
            mColsEval.setHeaders(_headers);
            if (!_isInitial) mColsEval.notInitial();
            auto res = mColsEval(x.lhs);
            if (res.second.size() > 0) return res;
            auto y = mColsEval(x.rhs);
            if (y.second.size() > 0) return y;
            res.first.add(y.first);
            return res;
        }
        result_type operator()(expr const& e) const {
            return boost::apply_visitor(*this, e);
        }

    };

    ///////////////////////////////////////////////////////////////////////////
    //  The AST evaluator
    ///////////////////////////////////////////////////////////////////////////
    struct evaluator {
    private:
        client::math::ast::evaluator _meval;
        client::str::ast::evaluator _seval;
    public:
        using mretFnT = client::math::ast::evaluator::retFnT;
        using sretFnT = client::str::ast::evaluator::retFnT;

        using retFnT = std::function<bool(const std::vector<std::string>&, const std::vector<double>&)>;
        evaluator(helper::positionTeller p, const helper::Global &g) : _meval{p, g}, _seval{p, g} {}
        evaluator(client::math::ast::evaluator m, client::str::ast::evaluator s) : 
            _meval{m}, _seval{s} {}
        typedef retFnT result_type;

        retFnT operator()(optoken const &o, mretFnT const &lhs, mretFnT const &rhs) const {
            switch (o)
            {
                case optoken::equal: return [lhs, rhs](const std::vector<std::string> &, const std::vector<double> &v) { return lhs(v) == rhs(v); };
                case optoken::not_equal: return [lhs, rhs](const std::vector<std::string> &, const std::vector<double> &v) { return lhs(v) != rhs(v); };
                case optoken::less: return [lhs, rhs](const std::vector<std::string> &, const std::vector<double> &v) { return lhs(v) < rhs(v); };
                case optoken::less_equal: return [lhs, rhs](const std::vector<std::string> &, const std::vector<double> &v) { return lhs(v) <= rhs(v); };
                case optoken::greater: return [lhs, rhs](const std::vector<std::string> &, const std::vector<double> &v) { return lhs(v) > rhs(v); };
                case optoken::greater_equal: return [lhs, rhs](const std::vector<std::string> &, const std::vector<double> &v) { return lhs(v) >= rhs(v); };
                default: BOOST_ASSERT(0); return result_type{};
            }
            BOOST_ASSERT(0);
            return result_type{};
        }

        retFnT operator()(mathOp const& x) const {
           return (*this)(x.operator_, _meval(x.lhs), _meval(x.rhs)); 
        }

        retFnT operator()(optoken const &o, sretFnT const &lhs, sretFnT const &rhs) const {
            switch (o) {
                case optoken::equal: return [lhs, rhs](const std::vector<std::string> &v, const std::vector<double> &) { return lhs(v) == rhs(v); };
                case optoken::not_equal: return [lhs, rhs](const std::vector<std::string> &v, const std::vector<double> &) { return lhs(v) != rhs(v); };
                case optoken::less: return [lhs, rhs](const std::vector<std::string> &v, const std::vector<double> &) { return lhs(v) < rhs(v); };
                case optoken::less_equal: return [lhs, rhs](const std::vector<std::string> &v, const std::vector<double> &) { return lhs(v) <= rhs(v); };
                case optoken::greater: return [lhs, rhs](const std::vector<std::string> &v, const std::vector<double> &) { return lhs(v) > rhs(v); };
                case optoken::greater_equal: return [lhs, rhs](const std::vector<std::string> &v, const std::vector<double> &) { return lhs(v) >= rhs(v); };
                default: BOOST_ASSERT(0); return result_type{};
            }
            BOOST_ASSERT(0);
            return result_type{};
        }

        retFnT operator()(strOp const& x) const {
           return (*this)(x.operator_, _seval(x.lhs), _seval(x.rhs)); 
        }

        retFnT operator()(expr const& x) const {
          return boost::apply_visitor(*this, x);
        }
    };

}}}

BOOST_FUSION_ADAPT_STRUCT(
    client::relational::ast::mathOp,
    (client::math::ast::expr, lhs)
    (client::relational::ast::optoken, operator_)
    (client::math::ast::expr, rhs)
)

BOOST_FUSION_ADAPT_STRUCT(
    client::relational::ast::strOp,
    (client::str::ast::expr, lhs)
    (client::relational::ast::optoken, operator_)
    (client::str::ast::expr, rhs)
)

#endif
