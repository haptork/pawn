/*!
 * @file
 * Function objects for basic piecemean/streaming reductions
 *
 * This file is a part of easyLambda(ezl) project for parallel data
 * processing with modern C++ and MPI.
 *
 * @copyright Utkarsh Bhardwaj <haptork@gmail.com> 2015-2016
 * Distributed under the Boost Software License, Version 1.0.
 * (See accompanying LICENSE.md or copy at * http://boost.org/LICENSE_1_0.txt)
 * */

#ifndef REDUCES_EZL_ALGO_H
#define REDUCES_EZL_ALGO_H

#include <tuple>

namespace ezl {

/*!
 * @ingroup algorithms
 * function object for applying multiple function each to a column
 *
 * */
template<class... Fns>
class FnsForEachCol {
public:
  FnsForEachCol(std::tuple<Fns...> &&fns) : _fns{fns} { }

  template <class... R, class... K, class V>
  auto operator()(const std::tuple<R...> &res, const std::tuple<K...> &key,
                  const V &val) {
    return _app(res, key, val, std::make_index_sequence<sizeof...(Fns)>{});
  };

private:
  template <class R, class K, class V, std::size_t... i>
  auto _app(const R &res, const K &key, const V &val, std::index_sequence<i...>) {
    return std::tuple_cat(std::get<i>(_fns)(std::make_tuple(std::get<i>(res)), key, std::make_tuple(std::get<i>(val)))...);
  }
  std::tuple<Fns...> _fns;
};

/*!
 * @ingroup algorithms
 * function object for applying multiple functions to all columns
 *
 * */
template<class... Fns>
class FnsForAllCol {
public:
  FnsForAllCol(std::tuple<Fns...> &&fns) : _fns{fns} { }

  template <class... R, class... K, class V>
  auto operator()(const std::tuple<R...> &res, const std::tuple<K...> &key,
                  const V &val) {
    return _app(res, key, val, std::make_index_sequence<sizeof...(Fns)>{});
  };

private:
  template <class R, class K, class V, std::size_t... i>
  auto _app(const R &res, const K &key, const V &val, std::index_sequence<i...>) {
    return std::tuple_cat(std::get<i>(_fns)(res, key, val)...);
  }
  std::tuple<Fns...> _fns;
};


template <class... Fns>
auto everyColFns(Fns... fns) {
  return FnsForAllCol<Fns...>(std::make_tuple(fns...));
}

template <class... Fns>
auto perColFns(Fns... fns) {
  return FnsForEachCol<Fns...>(std::make_tuple(fns...));
}

template <class Fn>
class Wrap {
public:
  Wrap(Fn &&fn) : _fn(fn) {}

  template<class... R, class... K, class V>
  auto operator () (const std::tuple<R...>& res, const std::tuple<K...>&, const V& val) {
    return _sum(
        res, val, std::make_index_sequence<std::tuple_size<V>::value>{});
  };

  template<class K, class V, class R>
  typename std::enable_if<!detail::meta::isTuple<R>{}, R>::type
  operator () (const R& res, const K&, const std::tuple<const V& >& val) {
    return _fn(res, std::get<0>(val));
  };

private:
  template <typename R, typename V, std::size_t... index>
  inline R _sum(const R &tup1, const V &tup2,
                  std::index_sequence<index...>) {
    return std::make_tuple(_fn(std::get<index>(tup1), (std::get<index>(tup2)))...);
  }

  Fn _fn;
};

template <class Fn>
class WrapPred {
public:
  WrapPred(Fn &&fn) : _fn(fn) {}

  template<class... R, class... K, class V>
  auto operator () (const std::tuple<R...>& res, const std::tuple<K...>&, const V& val) {
    return _sum(
        res, val, std::make_index_sequence<std::tuple_size<V>::value>{});
  };

  template<class K, class V, class R>
  typename std::enable_if<!detail::meta::isTuple<R>{}, R>::type
  operator () (const R& res, const K&, const std::tuple<const V& >& val) {
    return _fn(res, std::get<0>(val)) ? res : std::get<0>(val);
  };

private:
  template <typename R, typename V, std::size_t... index>
  inline R _sum(const R &tup1, const V &tup2,
                  std::index_sequence<index...>) {
    return std::make_tuple(_fn(std::get<index>(tup1), std::get<index>(tup2)) ? std::get<index>(tup1) : std::get<index>(tup2)...);
  }

  Fn _fn;
};

// usage: wrapBiFnReduce(std::plus<double>())
template <class T>
auto wrapBiFnReduce(T &&t) {
  return Wrap<T>{std::forward<T>(t)};
}

// usage: wrapPredReduce(std::greater<int>())
template <class T>
auto wrapPredReduce(T &&t) {
  return WrapPred<T>{std::forward<T>(t)};
}

/*!
 * @ingroup algorithms
 * function object for returning count of rows irrespective of key/value types.
 *
 * Example usage: 
 * @code
 * load<string>("*.txt").colSeparator("").rowSeparator('s')
 * .reduce<1>(ezl::count()).build()
 * @endcode
 *
 * */
class count {
public:
  template<class K, class V, class R>
  auto operator () (const R& res, const K&, const V&) {
    return 1 + res; //std::get<0>(res) + 1;
  }
};

class sum {
public:
  template<class... R, class... K, class V>
  auto operator () (const std::tuple<R...>& res, const std::tuple<K...>&, const V& val) {
    return _sum(
        res, val, std::make_index_sequence<std::tuple_size<V>::value>{});
  };

  template<class K, class V, class R>
  typename std::enable_if<!detail::meta::isTuple<R>{}, R>::type
  operator () (const R& res, const K&, const std::tuple<const V& >& val) {
    return res + std::get<0>(val);
  };

private:
  template <typename R, typename V, std::size_t... index>
  inline R _sum(const R &tup1, const V &tup2,
                  std::index_sequence<index...>) {
    return std::make_tuple(std::get<index>(tup1) + (std::get<index>(tup2))...);
  }
};

} // namespace ezl

#endif // !REDUCES_EZL_ALGO_H
