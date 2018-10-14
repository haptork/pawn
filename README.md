# pawn
ezl script for parallel data-processing from command line

based on easyLambda (ezl) (https://github.com/haptork/easyLambda)

To build and run on the example data:
- Dependencies:
  * c++14 compatible compiler (gcc-6 or more or similar).
  * boost c++ libraries (1.60 or above)
  * mpic++
- run `make` from the project directory. 
make should finish with some warnings that can be ignored. The executable is built in the bin directory. You might have to manually make a bin directory
execute with `./bin/pawn`
then you can type in some queries from `test_queries.txt` which run on the data given in data directory
to run on multiple processes run with command `mpirun -n 4 ./bin/pawn`
if there is a cluster you can use `qsub` or similar to invoke scheduler like for a usual mpi program

### Contributing

the code use boost-spirit parser, to parse the query, individual tests are given for each sub-expression parsing. Each sub-expression has mainly four files, for e.g. for math expressions like '5 + $2 / 4' there are the following files:
- include/mexpr.hpp : the file to be included for grammar definition
- include/mexpr_def.hpp : the implementation file for grammar definition
- include/mast.hpp: the ast (abstract syntax tree) for grammar, its printer and evaluation. The evaluation is in two steps, one is the evaluation of columns involved e.g. $2 * 4 + $3 uses 2nd and 3rd column so the column evaluation should return 2nd and 3rd. The other one is function evaluation which returns a function which given a vector (columns) returns the result for the values in the vector based on the expression.
- src/mexpr.cpp: helper for making object file of the math grammar.
and test file:
- mexpr_test.cpp: unit test file for math expression only
similarly for grammar for logical expressions, relational expressions etc. are there
then there are grammar for where sub expression, map, reduce, file etc. sub expressions that use the above grammars
I will try to add more documentation but till then please let me know if anything specific needs to be elaborated
