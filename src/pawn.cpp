/*!
 * @file
 * 
 *
 * command to run:
 * mpirun -n 2 ./bin/pawn
 *
 * */

/*
#include <random>
#include <stdexcept>

#include <boost/mpi.hpp>
#include <boost/algorithm/string/split.hpp>

#include <ezl.hpp>
#include <ezl/algorithms/predicates.hpp>

#include "pawn/exprParser.hpp"
#include "pawn/fromFilePawn.hpp"
*/

/**
 * fromFile junk | $x = $1 + ($2 * $3) | where ($2 == 5.0 and $1 > $2) or ($x >= 0.1) | reduce($5, $6) $y=sum $x
 * 
 * 
file junk|%x =+$3 $4|%y =*+$3 $4 $3|reduce($1, $2) %y=sum $3|show
file junk|%x =+*$3 $4 $3|show
file junk|%x =++$4 $3 $4|show
 * */
// using dataT = std::tuple<const std::vector<std::string>&, const std::vector<double>&>;
// using sourceT = std::shared_ptr<ezl::Source<dataT>>;

/*
struct ColIndices {
  std::vector<int> str;
  std::vector<int> num;
};

using Ops = std::vector<std::string>;

auto isShow(Ops ops, size_t i) {
  if (i == ops.size() - 2) {
    return true;
  }
  return false;
}

auto getSource(Ops ops, ColIndices indices, std::vector<int> workers) {
  using std::string; using std::vector;
  using ezl::rise; using ezl::fromFilePawn;
  auto &op = ops[0];
  vector<string> args;
  boost::split(args, op, boost::is_any_of(" \t"));
  // auto &cmd = args[0];
  // std::transform(begin(cmd), end(cmd), begin(cmd), tolower);
  // assert(cmd == "file"); only possibility rightnow
  auto &inFile = args[1];
  auto x = rise(fromFilePawn(inFile, indices.str, indices.num)).prll(workers);
  if (isShow(ops, 0)) x.dump();
  return x.build();
}

auto mapCols(std::string op) {
  auto res = std::vector<int>{};
  auto it = std::find(begin(op), end(op), '$');
  while (it != std::end(op)) {
    auto x = extractInt(it + 1, std::end(op));
    res.push_back(x);
    it = std::find(it + 1, end(op), '$');
  }
  return res;
}

auto reduceCols(std::string op, ColIndices& result) {
  auto sep = std::find(begin(op), end(op), '=');
  auto it = std::find(begin(op), sep, '$');
  while (it != sep) {
    auto x = extractInt(it + 1, sep);
    result.str.push_back(x);
    it = std::find(it + 1, sep, '$');
  }
  it = std::find(sep, end(op), '$');
  while (it != std::end(op)) {
    auto x = extractInt(it + 1, std::end(op));
    result.num.push_back(x);
    it = std::find(it + 1, end(op), '$');
  }
}

auto getColumnNumbers(std::vector<std::string> ops) {
  ColIndices cols;
  for (size_t i = 1; i < ops.size() - 1; ++i) {
    auto cmd = ops[i];
    if (cmd[0] == '%') {  // true for all rightnow
      auto x = mapCols(ops[i]);
      cols.num.insert(end(cols.num), begin(x), end(x));
    } else {
      reduceCols(ops[i], cols);
    }
  }
  std::set<int> temp {begin(cols.num), end(cols.num)};
  cols.num = std::vector<int>{begin(temp), end(temp)};
  std::set<int> temp2 {begin(cols.str), end(cols.str)};
  cols.str = std::vector<int>{begin(temp2), end(temp2)};
  return cols;
}

auto getOps(std::string line) {
  std::vector<std::string> ops;
  boost::split(ops, line, boost::is_any_of("|"));
  return ops;
}

sourceT addMap(sourceT src, Ops ops, int i, ColIndices cols) {
  auto op = ops[i];
  op.erase(begin(op), std::find(begin(op), end(op), '=') + 1);
 // auto fn = parseMathExpr(op.begin(), op.end(), cols.num);
  auto x = ezl::flow(src).map<2>([op, cols](std::vector<double> v) {
    auto fn = parseMathExpr(op.begin(), op.end(), cols.num);
    v.push_back(fn(v));
    return std::make_tuple(v);
  }).colsTransform();
  if (isShow(ops, i)) x.dump(); 
  return x.build();
 }

sourceT addReduce(sourceT src, Ops ops, int i, ColIndices cols) {
  using std::string; using std::vector; using std::tuple;
  auto op = ops[i];
  op.erase(begin(op), find(begin(op), end(op), '=') + 1);
  auto initial = tuple<vector<double>>{};
  std::get<0>(initial).push_back(0);
  // if (cols.str.empty()) {
  //   auto x = ezl::flow(src).reduce<ezl::key<>, ezl::val<2>>([op, cols](std::tuple<std::vector<double>>& res, std::vector<double> cur) -> std::tuple<vector<double>>& {
  //     //auto fn = parseRedExpr(polish.begin(), polish.end(), cols.num);
  //     std::get<0>(res)[0] += cur[0];
  //     return res;
  //   }, initial);
  //   if (isShow(ops, i)) x.dump(); 
  //   return x.build();
  // }
  auto x = ezl::flow(src).reduce<1>([op, cols](tuple<vector<double>>& res, const vector<string>& key, const vector<double> &cur) -> auto& {
    //auto fn = parseRedExpr(polish.begin(), polish.end(), cols.num);
    std::get<0>(res)[0] += cur[0];
    return res;
  }, std::move(initial)).inprocess();
  std::get<0>(initial).push_back(0);
  auto y = x.reduce<1>([op, cols](tuple<vector<double>>& res, const vector<string>& key, const vector<double> &cur) -> auto& {
    //auto fn = parseRedExpr(polish.begin(), polish.end(), cols.num);
    std::get<0>(res)[0] += cur[0];
    return res;
  }, std::move(initial)).prll({0}, ezl::llmode::task);
  if (isShow(ops, i)) y.dump(); 
  return y.build();
}

auto addUnit(sourceT src, std::vector<std::string> ops, int i, ColIndices cols) {
  using std::string; using std::vector; using ezl::flow; 
  auto &op = ops[i];
  vector<string> args;
  boost::split(args, op, boost::is_any_of(" \t"));
  auto &cmd = args[0];
  std::transform(begin(cmd), end(cmd), begin(cmd), tolower);
  if(cmd[0] == '%') {
    return addMap(src, ops, i, cols);
  } else {
    return addReduce(src, ops, i, cols);
  }
}

auto runFlow(sourceT src, std::vector<std::string> ops, ColIndices cols, std::vector<int> workers) {
  std::vector<int> all = workers;
  all.push_back(0);
  ezl::flow(src).run(all);
}

auto readQuery(std::string line, std::vector<int> workers) {
  Ops ops = getOps(line);
  if (ops.empty()) return false;
  ColIndices cols = getColumnNumbers(ops);
  sourceT src = getSource(ops, cols, workers);
  sourceT cur = src;
  for (size_t i = 1; i < ops.size() - 1; ++i) {
    cur = addUnit(cur, ops, i, cols);
  }
  runFlow(cur, ops, cols, workers);
  return true;
}

auto checkQuery(std::string line) {
  return std::string{};
}

auto pawn(int argc, char *argv[]) {
  int master = 0;
  auto nProc = ezl::Karta::inst().nProc();
  std::vector<int> workers(ezl::Karta::inst().nProc() - 1);
  if (nProc == 1) {
    if (workers.empty()) workers.push_back(0); // clang
    else workers[0] = 0; // gcc
  }
  else std::iota(begin(workers), end(workers), 1);
  auto scheduler = [](std::vector<int> workers) {
    return workers;
  };
  //workers.pop_back();
  //std::cout << workers << std::endl;
  auto queryCin = [&scheduler, &workers]{
    std::cout << "> ";
    std::string line;
    std::getline(std::cin, line);
    if (line == "exit") return std::make_pair(line, false);
    auto err = checkQuery(line);
    if (err.length() != 0) {
      std::cout << err;
      line = "";
    }
    auto curWorkers = scheduler(workers);
    return std::make_pair(line, true);
  };
  ezl::rise(queryCin).prll({master})
  .filter([&workers](std::string line) {
    if (line == "") return false;
    return readQuery(line, workers);
  }).prll(1.0, ezl::llmode::task | ezl::llmode::dupe)
  .run();
}
*/
/*
auto pawkSerial(int argc, char *argv[]) {
  int master = 0;
  auto nProc = ezl::Karta::inst().nProc();
  std::vector<int> workers(ezl::Karta::inst().nProc() - 1);
  if (nProc == 1) workers[0] = 0;
  else std::iota(begin(workers), end(workers), 1);
  auto scheduler = [](std::vector<int> workers) {
    return workers;
  };
  auto queryCin = [&scheduler, &workers]{
    std::cout << "> ";
    std::string line;
    std::getline(std::cin, line);
    if (line == "exit") return std::make_pair(line, false);
    auto err = checkQuery(line);
    if(err.length() != 0) {
      std::cout << err;
      line = "";
    }
    auto curWorkers = scheduler(workers);
    return std::make_pair(line, true);
  };
  while (true) {
    auto x = queryCin();
    if (x.second == false) break;
    readQuery(x.first, workers);
  }
}
*/
/*
int main(int argc, char *argv[]) {
  boost::mpi::environment env(argc, argv, false);
  try {
    pawn(argc, argv);
  } catch (const std::exception& ex) {
    std::cerr<<"error: "<<ex.what()<<'\n';
    env.abort(1);  
  } catch (...) {
    std::cerr<<"unknown exception\n";
    env.abort(2);  
  }
  return 0;
}
*/