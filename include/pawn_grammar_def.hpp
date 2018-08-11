#include "pawn_grammar.hpp"
//#include "error_handler.hpp"
#include <boost/spirit/include/phoenix_function.hpp>

namespace client { namespace pawn { namespace parser
{
    template <typename Iterator>
    expression<Iterator>::expression(error_handler<Iterator>& error_handler)
      : expression::base_type(expr), mathExpr{error_handler}, logicalExpr{error_handler}
      , reduceExpr{error_handler}, logicalCmd{error_handler}
    {
        qi::_1_type _1;
        qi::_2_type _2;
        qi::_3_type _3;
        qi::_4_type _4;

        qi::char_type char_;
        qi::uint_type uint_;
        qi::_val_type _val;
        qi::raw_type raw;
        qi::lexeme_type lexeme;
        qi::alpha_type alpha;
        qi::alnum_type alnum;
        qi::bool_type bool_;
        qi::double_type double_;

        using qi::on_error;
        using qi::on_success;
        using qi::fail;
        using boost::phoenix::function;

        typedef function<client::error_handler<Iterator> > error_handler_function;

        expr = src >> +('|' >> unit) >> '|' >> terminal;

        src = "file" >> quoted_string;

        quoted_string = raw[lexeme['"' >> +(char_ - '"') >> '"']];

        unit = '$' >> identifier >> '=' >> mathExpr
             | "where" >> (logicalExpr | logicalCmd)
             | "reduce" >> reduceCols >> reduceExpr
             | "zip" >> zipExpr;

        zipExpr = reduceCols >> '('  >> src >> *('|' >> unit) >> ')'; ;
        
        reduceCols = *(strOperand);

        identifier =  raw[lexeme[(alpha | '_') >> *(alnum | '_')]];

        strOperand = '%' >> (identifier | uint_);

        saveStr = strOperand >> "as" >> '%' >> identifier;

        saveNum = (('$' >> identifier | ('$' >> uint_)) >> "as" >> '$' >> identifier);

        saveVal = "saveVal" > +(saveNum | saveStr);

        fileName = "show" > lexeme[*(char_)]; 

        queryName = "saveQueryAs" > lexeme[(alpha | '_') > *(alnum | '_')];
        
        terminal = fileName
                 | queryName
                 | saveVal;

        ///////////////////////////////////////////////////////////////////////
        // Debugging and error handling and reporting support.
        BOOST_SPIRIT_DEBUG_NODES(
            (expr)
            (src)
            (quoted_string)
            (zipExpr)
            (unit)
            (reduceCols)
            (identifier)
            (strOperand)
            (saveStr)
            (saveNum)
            (saveVal)
            (fileName)
            (queryName)
            (terminal)
        );

        ///////////////////////////////////////////////////////////////////////
        // Error handling: on error in expr, call error_handler.
        on_error<fail>(expr,
            error_handler_function(error_handler)(
                _4, _3, _2));

        ///////////////////////////////////////////////////////////////////////
        // Annotation: on success in primary_expr, call annotation.
        /*
        on_success(primary_expr,
            annotation_function(error_handler.iters)(_val, _1));
            */
    }
}}}
