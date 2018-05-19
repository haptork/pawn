#if !defined(PAWN_GRAMMAR_HPP)
#define PAWN_GRAMMAR_HPP

#define BOOST_SPIRIT_NO_PREDEFINED_TERMINALS

#include <boost/spirit/include/qi.hpp>
#include "pawn_ast.hpp"
#include "mexpr.hpp"
#include "lexpr.hpp"
#include "aexpr.hpp"
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
        qi::rule<Iterator, ast::zipExpr(), ascii::space_type> zipExpr;
        qi::rule<Iterator, ast::quoted_stringT(), ascii::space_type> quoted_string;
        qi::rule<Iterator, ast::unit(), ascii::space_type> unit;
        qi::rule<Iterator, ast::identifierT(), ascii::space_type> identifier;
        qi::rule<Iterator, ast::strIdentifierT(), ascii::space_type> strIdentifier;
        qi::rule<Iterator, std::vector<unsigned int>(), ascii::space_type> reduceCols;
        qi::rule<Iterator, ast::saveStr(), ascii::space_type> saveStr;
        qi::rule<Iterator, ast::saveNum(), ascii::space_type> saveNum;
        qi::rule<Iterator, ast::saveVal(), ascii::space_type> saveVal;
        qi::rule<Iterator, ast::fileName(), ascii::space_type> fileName;
        qi::rule<Iterator, ast::queryName(), ascii::space_type> queryName;
        qi::rule<Iterator, ast::terminal(), ascii::space_type> terminal;
        client::math::parser::expression<Iterator> mathExpr;
        client::logical::parser::expression<Iterator> logicalExpr;
        client::reduce::parser::expression<Iterator> reduceExpr;
    };
}}}

#endif