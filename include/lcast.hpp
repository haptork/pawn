/*=============================================================================
    Copyright (c) 2001-2011 Joel de Guzman

    Distributed under the Boost Software License, Version 1.0. (See accompanying
    file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
=============================================================================*/
#if !defined(PAWN_LCAST_HPP)
#define PAWN_LCAST_HPP

#include <boost/config/warning_disable.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/fusion/include/adapt_struct.hpp>
#include <boost/fusion/include/io.hpp>
#include <boost/optional.hpp>
#include <list>
#include <map>
#include <dlfcn.h>

#include <helper.hpp>

namespace client { namespace logicalc { namespace ast {

    struct quoted {
      std::string val; 
    };
 
    struct expr {
      quoted path;
      std::string fn;
    };

    struct printer {
    public:
        void operator()(expr const& x) const {
            std::cout << x.path.val;
            std::cout << ' ';
            std::cout << x.fn;
        }
    };

    struct colsEval {
    private:
        using ColIndices = client::helper::ColIndices;
    public:
        using result_type = std::pair<ColIndices, std::string>;
        using retFnT = std::function<bool(const std::vector<std::string>&, const std::vector<double>&)>;
        colsEval(const ColIndices &, const helper::Global &) {}
        colsEval() {}
        void setHeaders(const std::vector<std::string>& h) {}
        void notInitial() {}
        result_type operator()(expr const& e) const {
            auto pLib = ::dlopen(e.path.val.c_str(), RTLD_LAZY);
            if (!pLib) {
                return std::make_pair(ColIndices{}, std::string{"Error opening " + e.path.val});
            }
            auto fn = ::dlsym(pLib, e.fn.c_str());
            if (!fn) {
                std::make_pair(ColIndices{}, std::string{"Error locating " + e.fn + " in " + e.path.val});
            }
            ::dlclose(pLib);
            return result_type{}; 
        }
    };

    struct whatever {
      whatever(bool x) : _x{x} {}
      auto operator() (const std::vector<std::string>&, const std::vector<double>&) { return _x; }
    private:
      bool _x;
    };
    struct evaluator {
    private:
        void *pLib = nullptr;
    public:
        using sigT = bool(const std::vector<std::string>&, const std::vector<double>&);
        using retFnT = std::function<sigT>;
        evaluator() {}
        evaluator(helper::positionTeller, const helper::Global &) {}
        typedef retFnT result_type;

        retFnT operator()(expr const& x)
        {
            pLib = ::dlopen(x.path.val.c_str(), RTLD_LAZY);
            if (!pLib) return whatever{true};
            auto fn = (sigT*)(::dlsym(pLib, x.fn.c_str()));
            if (!fn) return whatever{true};
            //::dlclose(pLib);
            return fn;//static_cast<sigT*>(fn);
        }

        ~evaluator() {
            if (!pLib or pLib == nullptr) return;
            ::dlclose(pLib);
        }
    };
}}}


BOOST_FUSION_ADAPT_STRUCT(
    client::logicalc::ast::quoted,
    (std::string, val)
)

BOOST_FUSION_ADAPT_STRUCT(
    client::logicalc::ast::expr,
    (client::logicalc::ast::quoted, path)
    (std::string, fn)
)
#endif
