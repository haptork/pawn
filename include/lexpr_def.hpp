#include "lexpr.hpp"
//#include "error_handler.hpp"
#include <boost/spirit/include/phoenix_function.hpp>

namespace client { namespace logical { namespace parser
{
    template <typename Iterator>
    expression<Iterator>::expression(error_handler<Iterator>& error_handler)
      : expression::base_type(expr), rexpr{error_handler}
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

        and_op.add
            ("&", ast::optoken::conjunct)
            ("&&", ast::optoken::conjunct)
            ("and", ast::optoken::conjunct)
            ;

        or_op.add
            ("|", ast::optoken::disjunct)
            ("||", ast::optoken::disjunct)
            ("or", ast::optoken::disjunct)
            ;

        unary_op.add
            ("!", ast::optoken::negate)
            ("~", ast::optoken::negate)
            ("not", ast::optoken::negate)
            ;

        ///////////////////////////////////////////////////////////////////////
        // Main expression grammar
        expr =
            or_expr.alias()
            ;
        
        or_expr =
                and_expr
            >> *(or_op >> and_expr)
            ;
 
        and_expr =
                unary_expr
            >> *(and_op >> unary_expr)
            ;

        unary_expr =
                primary_expr
            |   (unary_op >> primary_expr)
            ;

        primary_expr =
                bool_
            |   rexpr
            |   '(' >> expr >> ')'
            ;

        // Debugging and error handling and reporting support.
        BOOST_SPIRIT_DEBUG_NODES(
            (expr)
            (or_expr)
            (and_expr)
            (unary_expr)
            (primary_expr)
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
