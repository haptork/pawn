#if !defined(PAWN_GRAMMAR_HPP)
#define PAWN_GRAMMAR_HPP

#define BOOST_SPIRIT_NO_PREDEFINED_TERMINALS

#include <boost/spirit/include/qi.hpp>
#include "pawn_ast.hpp"
#include "mexpr.hpp"
#include "lexpr.hpp"
#include "error_handler.hpp"
#include <vector>

namespace client { namespace pawn { namespace parser
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
        qi::rule<Iterator, ast::src(), ascii::space_type> src;
        qi::rule<Iterator, std::string(), ascii::space_type> quoted_string;
        qi::rule<Iterator, ast::unit(), ascii::space_type> unit;
        qi::rule<Iterator, std::string(), ascii::space_type> identifier;
        qi::rule<Iterator, ascii::space_type> terminal;
        client::math::parser::expression<Iterator> mathExpr;
        client::logical::parser::expression<Iterator> logicalExpr;
    };
}}}

#endif