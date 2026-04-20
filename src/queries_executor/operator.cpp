#include <queries_executor/operator.h>

ScanOperator::ScanOperator(std::istream& data_reader_stream, const std::vector<std::string>& col_names) : data_reader(data_reader_stream), 
column_names(col_names) {
    type = OperatorType::SCAN;
}

// CountOperator::CountOperator(std::shared_ptr<Operator> child_op) {
//     type = OperatorType::COUNT;
//     child = child_op;
// }


FilterOperator::FilterOperator(std::shared_ptr<Operator> child_op, const std::string& column_name_, std::string&& value_) : value(std::move(value_)) {
    type = OperatorType::FILTER;
    child = child_op;
    column_name = column_name_;
}

AggregateOperator::AggregateOperator(std::shared_ptr<Operator> child_op, std::vector<std::shared_ptr<Aggregation>> aggregations) : aggs(std::move(aggregations)) {
    type = OperatorType::AGGREGATION;
    child = child_op;
}
