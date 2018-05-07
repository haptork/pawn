#if !defined(PAWN_MEXPR_HPP)
#define PAWN_MEXPR_HPP

#define BOOST_SPIRIT_NO_PREDEFINED_TERMINALS

#include <boost/spirit/include/qi.hpp>
#include "mast.hpp"
#include "error_handler.hpp"
#include <vector>

namespace client { namespace math { namespace parser
{
    namespace qi = boost::spirit::qi;
    namespace ascii = boost::spirit::ascii;

    ///////////////////////////////////////////////////////////////////////////////
    //  The math expression grammar
    ///////////////////////////////////////////////////////////////////////////////
    template <typename Iterator>
    struct expression : qi::grammar<Iterator, ast::expr(), ascii::space_type>
    {
        expression(error_handler<Iterator>& error_handler);

        qi::rule<Iterator, ast::expr(), ascii::space_type> 
          expr, additive_expr, multiplicative_expr;
        qi::rule<Iterator, ast::operand(), ascii::space_type> 
          unary_expr, primary_expr;
        qi::rule<Iterator, std::string(), ascii::space_type> identifier;
        qi::rule<Iterator, unsigned int, ascii::space_type> colIndex;
        qi::symbols<char, ast::optoken> additive_op, multiplicative_op, unary_op;
        qi::symbols<char>
            keywords
            ;
    };
}}}

#endif