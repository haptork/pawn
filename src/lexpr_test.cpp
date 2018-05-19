#include <iostream>
#include <map>
#include <numeric>

#include <last.hpp>
#include <lexpr.hpp>
#include <helper.hpp>

int testLogical() {
    std::cout << "/////////////////////////////////////////////////////////\n\n";
    std::cout << "Logical Expression parser...\n\n";
    std::cout << "/////////////////////////////////////////////////////////\n\n";
    std::cout << "Type an expression...or [q or Q] to quit\n\n";

    typedef std::string::const_iterator iterator_type;
    typedef client::logical::parser::expression<iterator_type> parser;
    typedef client::logical::ast::expr ast_expression;
    typedef client::logical::ast::printer ast_printer;
    // typedef client::math::ast::evaluator math_evaluator;
    // typedef client::relational::ast::evaluator rel_evaluator;
    typedef client::logical::ast::evaluator ast_evaluator;
    typedef client::logical::ast::colsEval cols_evaluator;
    typedef client::helper::positionTeller posTeller;

    std::string str;
    while (std::getline(std::cin, str))
    {
        if (str.empty() || str[0] == 'q' || str[0] == 'Q')
            break;

        std::vector<size_t> colIndices;
        colIndices.resize(9);
        std::iota(std::begin(colIndices), std::end(colIndices), 1);
        std::vector<std::string> vars {"xyz"};

        ast_printer print;
        client::helper::ColIndices cl;
        client::helper::Global g;
        cl.num = colIndices;
        cl.var = vars;
        
        ast_evaluator eval{posTeller{cl}, g};
        cols_evaluator cols{cl, g};

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
              auto e = eval(expression);
              std::vector<double> v;
              std::vector<std::string> u;
              v.resize(10);
              std::iota(std::begin(v), std::end(v), 0);
              std::cout << "result: " << std::boolalpha << e(u, v);
              std::cout << "-------------------------\n";
            }
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
