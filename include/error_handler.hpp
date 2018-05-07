/*=============================================================================
    Copyright (c) 2001-2011 Joel de Guzman

    Distributed under the Boost Software License, Version 1.0. (See accompanying
    file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
=============================================================================*/
#if !defined(BOOST_SPIRIT_CALC8_ERROR_HANDLER_HPP)
#define BOOST_SPIRIT_CALC8_ERROR_HANDLER_HPP

#include <iostream>
#include <string>
#include <vector>

namespace client
{
    namespace qi = boost::spirit::qi;
    namespace ascii = boost::spirit::ascii;
    using boost::phoenix::function;

    ///////////////////////////////////////////////////////////////////////////////
    //  The error handler
    ///////////////////////////////////////////////////////////////////////////////
    template <typename Iterator>
    struct error_handler
    {
        template <typename, typename, typename>
        struct result { typedef void type; };

        void operator()(
            qi::info const& what
          , Iterator err_pos, Iterator last) const
        {
            std::cout
                << "Error! Expecting "
                << what                         // what failed?
                << " here: \""
                << std::string(err_pos, last)   // iterators to error-pos, end
                << "\""
                << std::endl
            ;
        }
    };

}

#endif