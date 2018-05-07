#if !defined(PAWN_LEXPR_HPP)
#define PAWN_LEXPR_HPP

#define BOOST_SPIRIT_NO_PREDEFINED_TERMINALS

#include <boost/spirit/include/qi.hpp>
#include "last.hpp"
#include "rexpr.hpp"
#include "error_handler.hpp"
#include <vector>

namespace client { namespace logical { namespace parser
{
    namespace qi = boost::spirit::qi;
    namespace ascii = boost::spirit::ascii;

    ///////////////////////////////////////////////////////////////////////////////
    //  The logical expression grammar
    ///////////////////////////////////////////////////////////////////////////////
    template <typename Iterator>
    struct expression : qi::grammar<Iterator, ast::expr(), ascii::space_type>
    {
        expression(error_handler<Iterator>& error_handler);

        client::relational::parser::expression<Iterator> rexpr;
        qi::rule<Iterator, ast::expr(), ascii::space_type> 
          expr, or_expr, and_expr;
        qi::rule<Iterator, ast::operand(), ascii::space_type> 
          unary_expr, primary_expr;
        qi::symbols<char, ast::optoken> and_op, or_op, unary_op;
    };
}}}

#endif