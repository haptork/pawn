#if !defined(PAWN_REXPR_HPP)
#define PAWN_REXPR_HPP

#define BOOST_SPIRIT_NO_PREDEFINED_TERMINALS

#include <boost/spirit/include/qi.hpp>
#include "rast.hpp"
#include "mexpr.hpp"
#include "error_handler.hpp"
#include <vector>

namespace client { namespace relational { namespace parser
{
    namespace qi = boost::spirit::qi;
    namespace ascii = boost::spirit::ascii;

    ///////////////////////////////////////////////////////////////////////////////
    //  The relational expression grammar
    ///////////////////////////////////////////////////////////////////////////////
    template <typename Iterator>
    struct expression : qi::grammar<Iterator, ast::expr(), ascii::space_type>
    {
        expression(error_handler<Iterator>& error_handler);

        qi::rule<Iterator, ast::expr(), ascii::space_type> expr;
        client::math::parser::expression<Iterator> mathExpr;
        qi::symbols<char, ast::optoken> relational_op;
    };
}}}

#endif