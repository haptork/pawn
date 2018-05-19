#if defined(_MSC_VER)
# pragma warning(disable: 4345)
#endif

#include "aexpr_def.hpp"

typedef std::string::const_iterator iterator_type;
template struct client::reduce::parser::expression<iterator_type>;
