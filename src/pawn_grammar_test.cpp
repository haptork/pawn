#include <iostream>
#include <map>
#include <numeric>

#include <pawn_ast.hpp>
#include <pawn_grammar.hpp>

int testPawn() {
    std::cout << "/////////////////////////////////////////////////////////\n\n";
    std::cout << "Pawn Expression parser...\n\n";
    std::cout << "/////////////////////////////////////////////////////////\n\n";
    std::cout << "Type an expression...or [q or Q] to quit\n\n";
    std::cout << "e.x.: file \"t\" | $xz = $1 + ($2 * 3) | where ($2 == 5.0 and $1 > $xz) | show\n\n";

    typedef std::string::const_iterator iterator_type;
    typedef client::pawn::parser::expression<iterator_type> parser;
    typedef client::pawn::ast::expr ast_expression;
    typedef client::pawn::ast::printer ast_printer;
    typedef client::pawn::ast::colsEval cols_evaluator;

    std::string str;
    while (std::getline(std::cin, str)) {
        if (str.empty() || str[0] == 'q' || str[0] == 'Q')
            break;

        ast_printer print;
        cols_evaluator cols;

        std::string::const_iterator iter = str.begin();
        std::string::const_iterator end = str.end();

        client::error_handler<iterator_type> error_handler;//(iter, end); // Our error handler
        parser calc{error_handler};            // Our grammar
        ast_expression expression;  // Our program (AST)

        boost::spirit::ascii::space_type space;
        bool r = phrase_parse(iter, end, calc, space, expression);

        if (r && iter == end)
        {
            std::cout << "-------------------------\n";
            std::cout << "Parsing succeeded\n";
            print(expression);
            std::cout << "\n";
            auto x = cols(expression);
            if (x.second.size() > 0) {
              std::cout << "Error: " << x.second << " used before declaration.\n";
            } else {
              for (auto& it : x.first) {
                it.uniq();
                it.sort();
              }
              for (auto& jt : x.first) {
                std::cout << "\nnum columns are: ";
                for (auto it : jt.num) {
                  std::cout << it << ", ";
                }
                std::cout << "\nstr columns are: ";
                for (auto it : jt.str) {
                  std::cout << it << ", ";
                }
                std::cout << "\nvar are: ";
                for (auto it : jt.var) {
                  std::cout << it << ", ";
                }
                std::cout << "\n----\n";
              }
            }
            /*
            auto e = eval(expression);
            std::vector<double> v;
            v.resize(10);
            std::iota(std::begin(v), std::end(v), 0);
            std::cout << "result: " << e(v);
            std::cout << "-------------------------\n";
            */
        }
        else
        {
            std::string rest(iter, end);
            std::cout << "-------------------------\n";
            std::cout << "Parsing failed\n";
            std::cout << "at: " << rest << '\n';
            std::cout << "-------------------------\n";
        }
    }

    std::cout << "Bye... :-) \n\n";
    return 0;
}
