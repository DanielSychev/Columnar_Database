#include <queries_executor/operator.h>

ScanOperator::ScanOperator(std::istream& data_reader_stream, const std::vector<std::string>& col_names) : data_reader(data_reader_stream), 
column_names(col_names) {
    type = OperatorType::SCAN;
}

FilterOperator::FilterOperator(std::shared_ptr<Operator> child_op, std::vector<std::string>&& column_names, std::vector<std::string>&& values, std::vector<CompareSign>&& signs) : column_names(std::move(column_names)), values(std::move(values)), signs(std::move(signs)) {
    type = OperatorType::FILTER;
    child = child_op;
}

TransformsOperator::TransformsOperator(std::shared_ptr<Operator> child_op, std::vector<std::shared_ptr<Transform>> transforms_)
    : transforms(std::move(transforms_)) {
    type = OperatorType::TRANSFORM;
    child = child_op;
}

AggregateOperator::AggregateOperator(std::shared_ptr<Operator> child_op, std::vector<std::shared_ptr<Aggregation>> aggregations) : aggs(std::move(aggregations)) {
    type = OperatorType::AGGREGATION;
    child = child_op;
}

GroupByOperator::GroupByOperator(std::shared_ptr<Operator> child_op, const std::vector<std::string>& group_by_columns_, const std::vector<std::shared_ptr<Aggregation>>& aggs_) : group_by_columns(group_by_columns_), aggs(aggs_) {
    type = OperatorType::GROUPBY;
    child = child_op;
}

OrderByOperator::OrderByOperator(std::shared_ptr<Operator> child_op, std::vector<std::string>&& column_names, bool descending_, size_t limit_, size_t offset_) : column_names(std::move(column_names)), descending(descending_), limit(limit_), offset(offset_) {
    type = OperatorType::ORDERBY;
    child = child_op;
}

LimitOperator::LimitOperator(std::shared_ptr<Operator> child_op, size_t limit_) : limit(limit_) {
    type = OperatorType::LIMIT;
    child = child_op;
}
