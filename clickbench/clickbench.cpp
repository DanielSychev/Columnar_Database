#include "engine/serialization/batch_serialization.h"
#include "queries_executor/aggregation.h"
#include "queries_executor/operator.h"
#include "queries_executor/executor.h"
#include "queries_executor/transform.h"
#include <fstream>
#include <memory>
#include <stdexcept>

namespace {
std::unique_ptr<std::ifstream> data_stream;
std::shared_ptr<ScanOperator> scanner;
}

auto MakeFilter(std::shared_ptr<Operator> child_op, std::vector<std::string>&& column_names, std::vector<std::string>&& values, std::vector<CompareSign>&& signs) {
    return std::make_shared<FilterOperator>(child_op, std::move(column_names), std::move(values), std::move(signs));
}

void MakeScanOperator() {
    std::string data_path = "//Users//mac//Columnar_Database//src//TestFiles//output.mf";
    data_stream = std::make_unique<std::ifstream>(data_path, std::ios::binary);
    if (!data_stream->is_open()) {
        throw std::runtime_error("cannot open data file: " + data_path);
    }
}

// SELECT COUNT(*) FROM hits;
std::shared_ptr<Operator> MakeQuery0() {
    std::vector<std::string> columns = {"AdvEngineID"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>()};
    return std::make_shared<AggregateOperator>(scanner, aggregations);
}

// SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;
std::shared_ptr<Operator> MakeQuery1() {
    std::vector<std::string> columns = {"AdvEngineID"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto filter = MakeFilter(scanner, {"AdvEngineID"}, {"0"}, {CompareSign::NOT_EQUAL});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>()};
    return std::make_shared<AggregateOperator>(filter, aggregations);
}

// SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;
std::shared_ptr<Operator> MakeQuery2() {
    std::vector<std::string> columns = {"AdvEngineID", "ResolutionWidth"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<SumAggregation>("AdvEngineID"), std::make_shared<CountAggregation>(), std::make_shared<AvgAggregation>("ResolutionWidth")};
    return std::make_shared<AggregateOperator>(scanner, aggregations);
}

// SELECT AVG(UserID) FROM hits;
std::shared_ptr<Operator> MakeQuery3() {
    std::vector<std::string> columns = {"UserID"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<AvgAggregation>("UserID")};
    return std::make_shared<AggregateOperator>(scanner, aggregations);
}

// SELECT COUNT(DISTINCT UserID) FROM hits;
std::shared_ptr<Operator> MakeQuery4() {
    std::vector<std::string> columns = {"UserID"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountDistinctAggregation>("UserID")};
    return std::make_shared<AggregateOperator>(scanner, aggregations);
}

// SELECT COUNT(DISTINCT SearchPhrase) FROM hits;
std::shared_ptr<Operator> MakeQuery5() {
    std::vector<std::string> columns = {"SearchPhrase"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountDistinctAggregation>("SearchPhrase")};
    return std::make_shared<AggregateOperator>(scanner, aggregations);
}

// SELECT MIN(EventDate), MAX(EventDate) FROM hits;
std::shared_ptr<Operator> MakeQuery6() {
    std::vector<std::string> columns = {"EventDate"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<MinAggregation>("EventDate"), std::make_shared<MaxAggregation>("EventDate")};
    return std::make_shared<AggregateOperator>(scanner, aggregations);
}

// SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID  ORDER BY COUNT(*) DESC;
std::shared_ptr<Operator> MakeQuery7() {
    std::vector<std::string> columns = {"AdvEngineID"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto filter = MakeFilter(scanner, {"AdvEngineID"}, {"0"}, {CompareSign::NOT_EQUAL});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>()};
    auto group_by = std::make_shared<GroupByOperator>(filter, std::vector<std::string>{"AdvEngineID"}, aggregations);
    return std::make_shared<OrderByOperator>(group_by, "COUNT(*)", true);
}

// SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery8() {
    std::vector<std::string> columns = {"RegionID", "UserID"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountDistinctAggregation>("UserID", "u")};
    auto group_by = std::make_shared<GroupByOperator>(scanner, std::vector<std::string>{"RegionID"}, aggregations);
    return std::make_shared<OrderByOperator>(group_by, "u", true, 10);
}

// SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery9() {
    std::vector<std::string> columns = {"RegionID", "AdvEngineID", "ResolutionWidth", "UserID"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{
        std::make_shared<SumAggregation>("AdvEngineID"), 
        std::make_shared<CountAggregation>("c"), 
        std::make_shared<AvgAggregation>("ResolutionWidth"), 
        std::make_shared<CountDistinctAggregation>("UserID")};
    auto group_by = std::make_shared<GroupByOperator>(scanner, std::vector<std::string>{"RegionID"}, aggregations);
    return std::make_shared<OrderByOperator>(group_by, "c", true, 10);
}

// SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery10() {
    std::vector<std::string> columns = {"MobilePhoneModel", "UserID"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto filter = MakeFilter(scanner, {"MobilePhoneModel"}, {""}, {CompareSign::NOT_EQUAL});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountDistinctAggregation>("UserID", "u")};
    auto group_by = std::make_shared<GroupByOperator>(filter, std::vector<std::string>{"MobilePhoneModel"}, aggregations);
    return std::make_shared<OrderByOperator>(group_by, "u", true, 10);
}

// SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery11() {
    std::vector<std::string> columns = {"MobilePhone", "MobilePhoneModel", "UserID"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto filter = MakeFilter(scanner, {"MobilePhoneModel"}, {""}, {CompareSign::NOT_EQUAL});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountDistinctAggregation>("UserID", "u")};
    auto group_by = std::make_shared<GroupByOperator>(filter, std::vector<std::string>{"MobilePhone", "MobilePhoneModel"}, aggregations);
    return std::make_shared<OrderByOperator>(group_by, "u", true, 10);
}

// SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery12() {
    std::vector<std::string> columns = {"SearchPhrase"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto filter = MakeFilter(scanner, {"SearchPhrase"}, {""}, {CompareSign::NOT_EQUAL});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>("c")};
    auto group_by = std::make_shared<GroupByOperator>(filter, std::vector<std::string>{"SearchPhrase"}, aggregations);
    return std::make_shared<OrderByOperator>(group_by, "c", true, 10);
}

// SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery13() {
    std::vector<std::string> columns = {"SearchPhrase", "UserID"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto filter = MakeFilter(scanner, {"SearchPhrase"}, {""}, {CompareSign::NOT_EQUAL});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountDistinctAggregation>("UserID", "u")};
    auto group_by = std::make_shared<GroupByOperator>(filter, std::vector<std::string>{"SearchPhrase"}, aggregations);
    return std::make_shared<OrderByOperator>(group_by, "u", true, 10);
}

// SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery14() {
    std::vector<std::string> columns = {"SearchEngineID", "SearchPhrase"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto filter = MakeFilter(scanner, {"SearchPhrase"}, {""}, {CompareSign::NOT_EQUAL});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>("c")};
    auto group_by = std::make_shared<GroupByOperator>(filter, std::vector<std::string>{"SearchEngineID", "SearchPhrase"}, aggregations);
    return std::make_shared<OrderByOperator>(group_by, "c", true, 10);
}

// SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery15() {
    std::vector<std::string> columns = {"UserID"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>()};
    auto group_by = std::make_shared<GroupByOperator>(scanner, std::vector<std::string>{"UserID"}, aggregations);
    return std::make_shared<OrderByOperator>(group_by, "COUNT(*)", true, 10);
}

// SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery16() {
    std::vector<std::string> columns = {"UserID", "SearchPhrase"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>()};
    auto group_by = std::make_shared<GroupByOperator>(scanner, std::vector<std::string>{"UserID", "SearchPhrase"}, aggregations);
    return std::make_shared<OrderByOperator>(group_by, "COUNT(*)", true, 10);
}

// SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;
std::shared_ptr<Operator> MakeQuery17() {
    std::vector<std::string> columns = {"UserID", "SearchPhrase"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>()};
    auto group_by = std::make_shared<GroupByOperator>(scanner, std::vector<std::string>{"UserID", "SearchPhrase"}, aggregations);
    return std::make_shared<LimitOperator>(group_by, 10);
}

// SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery18() {
    std::vector<std::string> columns = {"UserID", "EventTime", "SearchPhrase"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto transforms = std::vector<std::shared_ptr<Transform>>{std::make_shared<ExtractMinuteTransform>("EventTime", "m")};
    auto transform = std::make_shared<TransformsOperator>(scanner, std::move(transforms));
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>()};
    auto group_by = std::make_shared<GroupByOperator>(transform, std::vector<std::string>{"UserID", "m", "SearchPhrase"}, aggregations);
    return std::make_shared<OrderByOperator>(group_by, "COUNT(*)", true, 10);
}

// SELECT UserID FROM hits WHERE UserID = 435090932899640449;
std::shared_ptr<Operator> MakeQuery19() {
    std::vector<std::string> columns = {"UserID"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto filter = MakeFilter(scanner, {"UserID"}, {"435090932899640449"}, {CompareSign::EQUAL});
    return filter;
}

// SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';
std::shared_ptr<Operator> MakeQuery20() {
    std::vector<std::string> columns = {"URL"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto filter = MakeFilter(scanner, {"URL"}, {"%google%"}, {CompareSign::LIKE});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>()};
    return std::make_shared<AggregateOperator>(filter, aggregations);
}

// SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery21() {
    std::vector<std::string> columns = {"URL", "SearchPhrase"};
    scanner = std::make_shared<ScanOperator>(*data_stream, columns);
    auto filter = MakeFilter(scanner, {"URL", "SearchPhrase"}, {"%google%", ""}, {CompareSign::LIKE, CompareSign::NOT_EQUAL});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<MinAggregation>("URL"), std::make_shared<CountAggregation>("c")};
    auto group_by = std::make_shared<GroupByOperator>(filter, std::vector<std::string>{"SearchPhrase"}, aggregations);
    return std::make_shared<OrderByOperator>(group_by, "c", true, 10);
}

int main() {
    MakeScanOperator();
    std::shared_ptr<Operator> queries[43];
    queries[0] = MakeQuery0();
    queries[1] = MakeQuery1();
    queries[2] = MakeQuery2();
    queries[3] = MakeQuery3();
    queries[4] = MakeQuery4();
    queries[5] = MakeQuery5();
    queries[6] = MakeQuery6();
    queries[7] = MakeQuery7();
    queries[8] = MakeQuery8();
    queries[9] = MakeQuery9();
    queries[10] = MakeQuery10();
    queries[11] = MakeQuery11();
    queries[12] = MakeQuery12();
    queries[13] = MakeQuery13();
    queries[14] = MakeQuery14();
    queries[15] = MakeQuery15();
    queries[16] = MakeQuery16();
    queries[17] = MakeQuery17();
    queries[18] = MakeQuery18();
    queries[19] = MakeQuery19();
    queries[20] = MakeQuery20();
    queries[21] = MakeQuery21();

    for (int i = 21; i < 22; ++i) {
        auto executor = ExecuteOperator(queries[i]);
        std::ofstream result_stream("//Users//mac//Columnar_Database//src//TestFiles//result"+std::to_string(i)+".csv");
        Writer result_writer(result_stream);
        while (auto batch = executor->NextBatch()) {
            batch_serialization::WriteCsvBatch(*batch, result_writer);
        }
    }
    return 0;
}
