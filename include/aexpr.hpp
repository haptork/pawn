#if !defined(PAWN_AEXPR_HPP)
#define PAWN_AEXPR_HPP

#define BOOST_SPIRIT_NO_PREDEFINED_TERMINALS

#include <boost/spirit/include/qi.hpp>
#include "aast.hpp"
#include "error_handler.hpp"
#include <vector>

namespace client { namespace reduce { namespace parser {
    namespace qi = boost::spirit::qi;
    namespace ascii = boost::spirit::ascii;

    ///////////////////////////////////////////////////////////////////////////////
    //  The math expression grammar
    ///////////////////////////////////////////////////////////////////////////////
    template <typename Iterator>
    struct expression : qi::grammar<Iterator, ast::expr(), ascii::space_type> {
        expression(error_handler<Iterator>& error_handler);

        qi::rule<Iterator, ast::expr(), ascii::space_type> expr;
        qi::rule<Iterator, ast::operation(), ascii::space_type> operation;
        qi::rule<Iterator, ast::operand(), ascii::space_type> operand;
        qi::rule<Iterator, std::string(), ascii::space_type> identifier;
        qi::rule<Iterator, unsigned int, ascii::space_type> colIndex;
        qi::symbols<char, ast::optoken> op;
    };
}}}

#endif