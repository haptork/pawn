#if !defined(PAWN_SEXPR_HPP)
#define PAWN_SEXPR_HPP

#define BOOST_SPIRIT_NO_PREDEFINED_TERMINALS

#include <boost/spirit/include/qi.hpp>
#include "sast.hpp"
#include "error_handler.hpp"
#include <vector>

namespace client { namespace str { namespace parser
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

        qi::rule<Iterator, ast::expr(), ascii::space_type> expr;
        qi::rule<Iterator, std::string(), ascii::space_type> identifier;
        qi::rule<Iterator, unsigned int, ascii::space_type> colIndex;
        qi::rule<Iterator, ast::quoted(), ascii::space_type> quoted_string;
    };
}}}

#endif