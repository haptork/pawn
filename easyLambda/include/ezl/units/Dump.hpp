/*!
 * @file
 * class Dump, unit for dumping to a file.
 *
 * This file is a part of easyLambda(ezl) project for parallel data
 * processing with modern C++ and MPI.
 *
 * @copyright Utkarsh Bhardwaj <haptork@gmail.com> 2015-2016
 * Distributed under the Boost Software License, Version 1.0.
 * (See accompanying LICENSE.md or copy at * http://boost.org/LICENSE_1_0.txt)
 * */

#ifndef DUMP_EZL_H
#define DUMP_EZL_H

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <tuple>

#include <ezl/pipeline/Dest.hpp>
#include <ezl/helper/meta/prettyprint.hpp>

namespace ezl {
namespace detail {

/*!
 * @ingroup units
 * A dead end for a pipeline branch dumps data to file(s) or output stream if
 * file name is not specified.
 *
 * If running on multiple processes filename is prefixed with rank.
 *
 * This is added to the pipeline when a unit specifies `dump` property.
 * `DumpExpr` has the builder expression for adding it for the current unit
 * in the pipeline.
 *
 * */
template <class I>
struct Dump : public Dest<I> {
public:
  using itype = I;
  static constexpr int isize = std::tuple_size<I>::value;

  Dump(std::string fname, std::string head) : _fname{fname}, _header{head} {
    _os = &std::cout;
    _fb = nullptr;
  }

  ~Dump() {
    if (_os != &std::cout) {
      delete _os;
    }
  }

  // unlike links there can be no cycle hence visited flag isn't needed.
  virtual void forwardPar(const Par *par) override final {
    if (_parred || !par->inRange()) return;
    _parred = true;
    if (par && _fname.length() > 0) {
      auto prefname = _fname;
      if (par->nProc() > 1) {
        auto dot = prefname.find_last_of(".");
        if (dot == std::string::npos) dot = prefname.size();
        prefname = prefname.substr(0, dot) + "_p" + std::to_string(par->rank()) +
                 prefname.substr(dot);
      }
      if (!_fb) _fb = std::make_unique<std::filebuf>();
      if ((_fb->is_open() && prefname != _fname) || !_fb->is_open()) {
        _fb->close();
        _fb->open(prefname, std::fstream::out|std::fstream::app);
        if(!_fb->is_open()) {
          Karta::inst().log("Can not write to file "+prefname, LogMode::warning);
        }
        _os = new std::ostream(_fb.get());
      }
    }
    if ((par->pos() == 0 || !_fname.empty()) && !_header.empty()) {
      (*_os)<<_header<<std::endl;
    }
  }

  virtual std::vector<Task *> forwardTasks() override final { return std::vector<Task *>{}; }

  virtual void dataEvent(const I &data) override final {
    //(*_os) << Karta::inst().rank() << ": " << data << '\n';
    (*_os) << data << '\n';
  }

  virtual void signalEvent(int i) override final {
    if (i == 0) this->incSig();
    else if (this->decSig() != 0) return;
    _parred = false;
    (*_os)<<std::flush;
    if (_fb) _fb->close();
    // resetting
    _os = &std::cout;
    _fb = nullptr;
  }

private:
  std::string _fname;
  std::string _header;
  std::unique_ptr<std::filebuf> _fb;
  std::ostream *_os;
  bool _parred{false};
};
}
} // namespace ezl ezl::detail

#endif // !DUMP_EZL_H
