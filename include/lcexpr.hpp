#if !defined(PAWN_LCEXPR_HPP)
#define PAWN_LCEXPR_HPP

#define BOOST_SPIRIT_NO_PREDEFINED_TERMINALS

#include <boost/spirit/include/qi.hpp>
#include "lcast.hpp"
#include "error_handler.hpp"
#include <vector>

namespace client { namespace logicalc { namespace parser
{
    namespace qi = boost::spirit::qi;
    namespace ascii = boost::spirit::ascii;

    ///////////////////////////////////////////////////////////////////////////////
    //  The logical expression grammar
    ///////////////////////////////////////////////////////////////////////////////
    template <typename Iterator>
    struct expression : qi::grammar<Iterator, ast::expr(), ascii::space_type> {
        expression(error_handler<Iterator>& error_handler);
        qi::rule<Iterator, ast::expr(), ascii::space_type> expr, logicalc_expr;
        qi::rule<Iterator, std::string(), ascii::space_type> identifier;
        qi::rule<Iterator, ast::quoted(), ascii::space_type> quoted_string;
    };
}}}

#endif