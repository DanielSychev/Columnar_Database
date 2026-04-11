#include <queries_executor/operator.h>

ScanOperator::ScanOperator(std::istream& data_reader_stream, const std::vector<std::string>& col_names) : data_reader(data_reader_stream), 
column_names(col_names) {
    type = OperatorType::SCAN;
}

CountOperator::CountOperator(std::shared_ptr<Operator> child_op) {
    type = OperatorType::COUNT;
    child = child_op;
}
