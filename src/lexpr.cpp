#if defined(_MSC_VER)
# pragma warning(disable: 4345)
#endif

#include "lexpr_def.hpp"

typedef std::string::const_iterator iterator_type;
template struct client::logical::parser::expression<iterator_type>;
