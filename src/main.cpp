#include <random>
#include <stdexcept>

#include <boost/mpi.hpp>
#include <boost/algorithm/string/split.hpp>

#include <ezl.hpp>
#include <fromFilePawn.hpp>

#include <helper.hpp>
#include <mast.hpp>
#include <last.hpp>
#include <pawn_ast.hpp>
#include <pawn_grammar.hpp>

std::string sanityCheck(const client::helper::ColIndices& cols) { 
  if (cols.num.empty() && cols.str.empty()) {
    return std::string{"There should be atleast one column index loaded from the file."};
  }
  return std::string{}; 
}

auto cookAst(std::string str) {
  using std::vector; using std::string; using std::tuple;
  using client::helper::ColIndices;
  typedef string::const_iterator iterator_type;
  typedef client::pawn::parser::expression<iterator_type> parser;
  typedef client::pawn::ast::expr ast_expression;
  typedef client::pawn::ast::colsEval cols_evaluator;
  //typedef client::pawn::ast::keysEval keys_evaluator;
  typedef std::pair<ast_expression, ColIndices> resultT;

  cols_evaluator cols;
  //keys_evaluator keys;

  iterator_type iter = str.begin();
  iterator_type end = str.end();

  client::error_handler<iterator_type> error_handler;
  parser calc{error_handler};
  resultT res;

  boost::spirit::ascii::space_type space;
  bool r = phrase_parse(iter, end, calc, space, std::get<0>(res));
  if (r && iter == end) {
      auto x = cols(std::get<0>(res));
      if (x.second.size() > 0) {
        std::cout << "Error: " << x.second << " used before declaration.\n";
        return std::make_pair(res, false);
      }
      std::string err = sanityCheck(x.first);
      if (err.size() > 0) {
        std::cout << err << '\n';
        return std::make_pair(res, false);
      }
      /*
      auto k = keys(std::get<0>(res));
      if (k.second.size() > 0) {
        std::cout << "Error: " << k.second << " used before declaration.\n";
        return std::make_pair(res, false);
      }
      std::move(std::begin(k.first), std::end(k.first), std::back_inserter(x.first.str));
      */
      x.first.uniq();
      x.first.sort();
      x.first.var = cols.variables();
      res.second = x.first;
      return std::make_pair(res, true);
  }
  std::string rest(iter, end);
  std::cout << "-------------------------\n";
  std::cout << "Syntax error at: " << rest << '\n';
  std::cout << "-------------------------\n";
  return std::make_pair(res, false);
}

auto getSource(std::string inFile, client::helper::ColIndices cols, std::vector<int> workers) {
  using ezl::rise; using ezl::fromFilePawn;
  return rise(fromFilePawn(inFile, cols.str, cols.num)).prll(workers).build();
}

using dataT = std::tuple<const std::vector<std::string>&, const std::vector<double>&>;
using sourceT = std::shared_ptr<ezl::Source<dataT>>;

struct AddUnits {
  typedef std::list<client::pawn::ast::unit> unitsT;
  typedef client::relational::ast::evaluator revalT;
  typedef client::pawn::ast::map mapT;
  typedef client::pawn::ast::filter filterT;
  typedef client::pawn::ast::reduce reduceT;
  typedef void result_type;
  sourceT _cur;
  client::helper::ColIndices _indices;
  client::helper::positionTeller _posTell;
  client::math::ast::evaluator _meval;
  client::logical::ast::evaluator _leval;
  client::reduce::ast::evaluator _aeval;
  bool _isShow {false};
  AddUnits() : _posTell{_indices}, _meval{_posTell}, 
               _leval{_posTell}, _aeval{_posTell} { }

  void operator()(mapT const &m) {
    auto fn = _meval(m.operation);
    auto x = ezl::flow(_cur).map<2>([fn](std::vector<double> v) {
      v.push_back(fn(v));
      return std::make_tuple(v);
    }).colsTransform();
    if (_isShow) x.dump(); 
    _cur = x.build();
  }

  void operator()(filterT const &f) {
    auto fn = _leval(f);
    auto x = ezl::flow(_cur).filter(std::move(fn));
    if (_isShow) x.dump(); 
    _cur = x.build();
  }

  void operator()(reduceT const &r) { 
    using std::tuple; using std::vector; using std::string;
    using resT = std::tuple<std::vector<double>>&;
    using keyT = const std::vector<std::string>&;
    using rowT = const std::vector<double>&;
    auto vf = _aeval(r.operation);
    auto fn = [vf](resT r, keyT k, rowT c) -> auto& { for (const auto &f : vf) f(r, k, c); return r; };
    auto initial = std::make_tuple(std::vector<double>(vf.size()));
    auto x = ezl::flow(_cur).reduce<1>(std::move(fn), std::move(initial)).inprocess();
    _aeval.sameIndex();
    vf = _aeval(r.operation);
    auto fn2 = [vf](resT r, keyT k, rowT c) -> auto& { for (const auto &f : vf) f(r, k, c); return r; };
    initial = std::make_tuple(std::vector<double>(vf.size()));
    auto y = x.reduce<1>(std::move(fn2), std::move(initial)).prll({0}, ezl::llmode::task);
    _aeval.sameIndex(false);
    if (_isShow) y.dump(); 
    _cur = y.build();
  }

  sourceT operator()(sourceT src, client::helper::ColIndices indices, unitsT units) {
    _indices.str = indices.str;
    _indices.num = indices.num;
    _indices.var = indices.var;
    _cur = src;
    _isShow = false;
    auto i = 0;
    for (const auto &it : units) {
      if (i++ == units.size() - 1) _isShow = true;
      boost::apply_visitor(*this, it);
    }
    return _cur;
  }
};

auto runFlow(sourceT src, std::vector<int> workers) {
  std::vector<int> all = workers;
  all.push_back(0);
  ezl::flow(src).run(all);
}

auto readQuery(std::string line, std::vector<int> workers) {
  using std::vector; using std::string; using std::tuple;
  using client::helper::ColIndices;
  typedef string::const_iterator iterator_type;
  typedef client::pawn::ast::expr ast_expression;
  ast_expression expression;
  ColIndices colIndices;
  std::tie(expression, colIndices) = cookAst(line).first;
  std::string fname{expression.first.begin() + 1, expression.first.end() - 1};
  sourceT src = getSource(fname, colIndices, workers);
  AddUnits addUnits;
  auto cur = addUnits(src, colIndices, expression.units);
  runFlow(cur, workers);
  return true;
}

auto pawn(int argc, char *argv[]) {
  int master = 0;
  auto nProc = ezl::Karta::inst().nProc();
  std::vector<int> workers;
  if (nProc == 1) {
    workers.push_back(0);
  } else {
    workers.resize(ezl::Karta::inst().nProc() - 1);
    std::iota(begin(workers), end(workers), 1);
  }
  /*
  auto scheduler = [](std::vector<int> workers) {
    return workers;
  };
  */
  ezl::Karta::inst().print0("\nType pawn expressions... or [q or Q] to quit");
  ezl::Karta::inst().print0("e.x.: file \"t\" | $xz = $1 + ($2 * 3) | where "
                            "($xz == 5.0 * 2 and $1 > $4 / 2) | show\n");
  auto queryCin = [&workers]{
    std::cout << "> ";
    std::string line;
    std::getline(std::cin, line);
    if (line.empty() || line[0] == 'q' || line[0] == 'Q') {
      return std::make_pair(line, false);
    }
    auto isGood = cookAst(line).second;
    if (!isGood) line = "";
    //auto curWorkers = scheduler(workers);
    return std::make_pair(line, true);
  };
  ezl::rise(queryCin).prll({master})
  .filter([&workers](std::string line) {
    if (line == "") return false;
    return readQuery(line, workers);
  }).prll(1.0, ezl::llmode::task | ezl::llmode::dupe)
  .run();
}

int main(int argc, char *argv[]) {
  boost::mpi::environment env(argc, argv, false);
  try {
    pawn(argc, argv);
  } catch (const std::exception& ex) {
    std::cerr << "error: "<<ex.what()<<'\n';
    env.abort(1);  
  } catch (...) {
    std::cerr << "unknown exception\n";
    env.abort(2);  
  }
  return 0;
}
