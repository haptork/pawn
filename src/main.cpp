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

using dataT = std::tuple<const std::vector<std::string>&, const std::vector<double>&>;
using sourceT = std::shared_ptr<ezl::Source<dataT>>;

std::vector<sourceT> sources;

sourceT internalZip(client::pawn::ast::zipExpr&, std::vector<int> workers, client::helper::Global &global);

std::string sanityCheck(const client::helper::ColIndices& cols) { 
  if (cols.num.empty() && cols.str.empty()) {
    return std::string{"There should be atleast one column index loaded from the file."};
  }
  return std::string{}; 
}

auto cookAst(std::string str, const client::helper::Global &global) {
  using std::vector; using std::string; using std::tuple;
  using client::helper::ColIndices;
  typedef string::const_iterator iterator_type;
  typedef client::pawn::parser::expression<iterator_type> parser;
  typedef client::pawn::ast::expr ast_expression;
  typedef client::pawn::ast::colsEval cols_evaluator;
  typedef ast_expression resultT;

  cols_evaluator cols{global};

  iterator_type iter = str.begin();
  iterator_type end = str.end();

  client::error_handler<iterator_type> error_handler;
  parser calc{error_handler};
  resultT res;

  boost::spirit::ascii::space_type space;
  bool r = phrase_parse(iter, end, calc, space, res);
  if (r && iter == end) {
      auto err = cols(res);
      if (err.size() > 0) {
        std::cout << err << '\n';
        return std::make_pair(res, false);
      }
      err = sanityCheck(res.first.colIndices);
      if (err.size() > 0) {
        std::cout << err << '\n';
        return std::make_pair(res, false);
      }
      return std::make_pair(res, true);
  }
  std::string rest(iter, end);
  std::cout << "-------------------------\n";
  std::cout << "Syntax error at: " << rest << '\n';
  std::cout << "-------------------------\n";
  return std::make_pair(res, false);
}

auto getSource(client::pawn::ast::src &s, std::vector<int> workers) {
  using ezl::rise; using ezl::fromFilePawn;
  std::string inFile{s.fname.begin() + 1, s.fname.end() - 1};
  return rise(fromFilePawn(inFile, s.colIndices.str, s.colIndices.num)).prll(workers).build();
}

struct AddUnits {
  typedef std::list<client::pawn::ast::unit> unitsT;
  typedef client::relational::ast::evaluator revalT;
  typedef client::pawn::ast::map mapT;
  typedef client::pawn::ast::filter filterT;
  typedef client::pawn::ast::reduce reduceT;
  typedef client::pawn::ast::zipExpr zipT;
  typedef void result_type;
  using mevalT = client::math::ast::evaluator;
  using levalT = client::logical::ast::evaluator;
  using aevalT = client::reduce::ast::evaluator;
  using ColIndices = client::helper::ColIndices;
  using Global = client::helper::Global;
  using positionTeller = client::helper::positionTeller;

  sourceT _cur;
  ColIndices _indices;
  positionTeller _posTell;

  mevalT _meval;
  levalT _leval;
  aevalT _aeval;
  bool _isShow {false};
  std::string _fname;
  bool _isDump;
  std::vector<int> _workers;
  Global &_global;
  AddUnits(std::string fn, bool isDump, std::vector<int> workers, Global &g) : _posTell{_indices}, _meval{_posTell, g},
          _leval{_posTell, g}, _aeval{_posTell}, _fname{fn}, _isDump{isDump}, _workers{workers}, _global{g} { }

  void operator()(mapT const &m) {
    auto fn = _meval(m.operation);
    auto x = ezl::flow(_cur).map<2>([fn](std::vector<double> v) {
      v.push_back(fn(v));
      return std::make_tuple(v);
    }).colsTransform();
    if (_isShow) x.dump(_fname); 
    _cur = x.build();
  }

  void operator()(filterT const &f) {
    auto fn = _leval(f);
    auto x = ezl::flow(_cur).filter(std::move(fn));
    if (_isShow) x.dump(_fname); 
    _cur = x.build();
  }

  void columnSelect(std::vector<unsigned int> vstr) {
    std::vector<int> keepIndices;
    for (auto it : vstr) {
      keepIndices.push_back(_posTell.str(it));
    }
    auto fn = [keepIndices](const std::vector<std::string> &s) { 
      std::vector<std::string> res;
      for (auto it : keepIndices) res.push_back(s[it]);
      return std::make_tuple(res);
    };
    _cur = ezl::flow(_cur).map<1>(std::move(fn)).colsTransform().build();
  }

  void operator()(zipT &r) { 
    using std::tuple; using std::vector; using std::string;
    using resT = std::tuple<std::vector<double>>&;
    using keyT = const std::vector<std::string>&;
    using rowT = const std::vector<double>&;
    if (r.cols.size() < _indices.str.size()) columnSelect(r.cols);
    auto fl = internalZip(r, _workers, _global);
    auto x = ezl::flow(_cur).zip<1>(std::move(fl)).prll({0}, ezl::llmode::task).colsDrop<3>()
               .map<2, 3>([](std::vector<double> v1, std::vector<double> v2) {
                 std::move(begin(v2), end(v2), std::back_inserter(v1));
                 return std::make_tuple(v1);
               }).colsTransform();
    _indices = r.colIndices;
    if (_isShow) x.dump(_fname); 
    _cur = x.build();
  }

  void operator()(reduceT const &r) { 
    using std::tuple; using std::vector; using std::string;
    using resT = std::tuple<std::vector<double>>&;
    using keyT = const std::vector<std::string>&;
    using rowT = const std::vector<double>&;
    if (r.cols.size() < _indices.str.size()) columnSelect(r.cols);
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
    _indices = r.colIndices;
    if (_isShow) y.dump(_fname); 
    _cur = y.build();
  }

  sourceT operator()(sourceT &src, client::helper::ColIndices &colIndices, std::list<client::pawn::ast::unit>& units) {
    _indices = colIndices;
    _cur = src;
    _isShow = false;
    auto i = 0;
    for (auto &it : units) {
      if (i++ == units.size() - 1) _isShow = _isDump;
      boost::apply_visitor(*this, it);
    }
    return _cur;
  }
};

struct saveValHelper {
private:
  const dataT _data;
  client::helper::positionTeller _posTell;
  client::helper::Global &_global;

public:
  saveValHelper(const dataT data, const client::helper::ColIndices &c, 
    client::helper::Global &g) : _data{data}, _posTell{c}, _global{g} { }

  using result_type = void;

  struct saveNumHelper {
  private:
    const dataT _data;
    client::helper::positionTeller _posTell;
  public:
    using result_type = double;
    saveNumHelper(const dataT data, const client::helper::positionTeller &c) 
      : _data{data}, _posTell{c} { }

    result_type operator()(std::string s) const {
      return std::get<1>(_data)[_posTell.var(s)];
    }

    result_type operator()(unsigned int s) const {
      return std::get<1>(_data)[_posTell.num(s)];
    }
  };

  void operator()(const client::pawn::ast::saveNum& s) {
    _global.gVarsN[s.dest] = boost::apply_visitor(saveNumHelper{_data, _posTell}, s.src);
  }

  void operator()(const client::pawn::ast::saveStr &s) {
    _global.gVarsS[s.dest] = std::get<0>(_data)[_posTell.str(s.src)];
  }

  void operator()(client::pawn::ast::saveVal const &s) {
    for(auto& it : s) {
      boost::apply_visitor(*this, it);
    }
  }

  void operator()(client::pawn::ast::fileName const &s) {}
  void operator()(client::pawn::ast::queryName const &s) {}
};

auto runFlow(sourceT src, std::vector<int> workers, bool isSaveVal,
    const client::pawn::ast::expr &expr, client::helper::Global &g) {
  auto &t = expr.last;
  auto &c = expr.colIndices;
  workers.push_back(0);
  if (isSaveVal) {
    auto x = ezl::flow(src)
      .filter([](const dataT&) { return true; })
        .prll(workers, ezl::llmode::task | ezl::llmode::dupe)
      .get(workers);
    auto y = saveValHelper{x[0], c, g};
    if (!x.empty()) boost::apply_visitor(y, t);
  } else {
    ezl::flow(src).run(workers);
  }
}

enum class terminalType : int {
  query,
  file,
  val
};

struct terminalInfoVisitor {
public:
  using queryName = client::pawn::ast::queryName;
  using fileName = client::pawn::ast::fileName;
  using saveVal = client::pawn::ast::saveVal;
  using result_type = std::pair<std::string, terminalType>;
  result_type operator()(const queryName& q) const { return std::make_pair(q.name, terminalType::query); } 
  result_type operator()(const fileName& f) const { return std::make_pair(f.name, terminalType::file); } 
  result_type operator()(const saveVal& s) const { return std::make_pair("", terminalType::val); }
};

sourceT internalZip(client::pawn::ast::zipExpr &expression, std::vector<int> workers, client::helper::Global &global) {
  sourceT src = getSource(expression.first,  workers);
  sources.push_back(src);
  AddUnits addUnits{"", false, workers, global};
  auto cur = addUnits(src, expression.first.colIndices, expression.units);
  return cur;
}

auto readQuery(std::string line, std::vector<int> workers, client::helper::Global& global) {
  using std::vector; using std::string; using std::tuple;
  using client::helper::ColIndices; using client::helper::Global;
  typedef string::const_iterator iterator_type;
  typedef client::pawn::ast::expr ast_expression;
  ast_expression expression;
  expression = cookAst(line, global).first;
  auto terminalInfo = boost::apply_visitor(terminalInfoVisitor{}, expression.last);
  if (terminalInfo.second == terminalType::query) {
    global.gQueries[terminalInfo.first] = line;
    return true;
  }
  sourceT src = getSource(expression.first, workers);
  AddUnits addUnits{terminalInfo.first, true, workers, global};
  auto cur = addUnits(src, expression.first.colIndices, expression.units);
  runFlow(cur, workers, terminalInfo.second == terminalType::val, expression, global);
  sources.clear();
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
  client::helper::Global global;
  auto queryCin = [&global]{
    std::cout << "> ";
    std::string line;
    std::getline(std::cin, line);
    if (line.empty() || line[0] == 'q' || line[0] == 'Q') {
      return std::make_pair(line, false);
    }
    auto isGood = cookAst(line, global).second;
    if (!isGood) line = "";
    //auto curWorkers = scheduler(workers);
    return std::make_pair(line, true);
  };
  ezl::rise(queryCin).prll({master})
  .filter([&workers, &global](std::string line) {
    if (line == "") return false;
    return readQuery(line, workers, global);
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
