#if defined(_MSC_VER)
# pragma warning(disable: 4345)
#endif

#include "mexpr_def.hpp"

typedef std::string::const_iterator iterator_type;
template struct client::math::parser::expression<iterator_type>;
