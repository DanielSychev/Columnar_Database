#include "engine/serialization/batch_serialization.h"
#include "queries_executor/aggregation.h"
#include "queries_executor/executor.h"
#include "queries_executor/operator.h"
#include "queries_executor/transform.h"
#include <chrono>
#include <cstddef>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {
constexpr std::string_view default_mf_input_path = "src/TestFiles/output.mf";
constexpr std::string_view default_result_directory = "src/TestFiles/result";
std::unique_ptr<std::ifstream> data_stream;

void Helper(const char* executable) {
    std::cout << "Usage:\n" << "  " << executable << " <query_num> [input.mf] [output.csv]\n";
}

std::string GetArgument(int argc, char** argv, int index, std::string_view default_value) {
    if (argc > index) {
        return argv[index];
    }
    return std::string(default_value);
}

void EnsureNoExtraArguments(int argc, int max_argc) {
    if (argc > max_argc) {
        throw std::runtime_error("too many arguments");
    }
}

std::string GetDefaultResultPath(size_t query_num) {
    return std::string(default_result_directory) + std::to_string(query_num) + ".csv";
}
}

auto MakeScan(std::vector<std::string>&& column_names) {
    return std::make_shared<ScanOperator>(*data_stream, column_names);
}

auto MakeFilter(std::shared_ptr<Operator> child_op, std::vector<std::string>&& column_names, std::vector<std::string>&& values, std::vector<CompareSign>&& signs) {
    return std::make_shared<FilterOperator>(child_op, std::move(column_names), std::move(values), std::move(signs));
}

auto MakeGroupBy(std::shared_ptr<Operator> child_op, std::vector<std::string>&& group_by_columns, const std::vector<std::shared_ptr<Aggregation>>& aggregations) {
    return std::make_shared<GroupByOperator>(child_op, group_by_columns, aggregations);
}

auto MakeOrderBy(std::shared_ptr<Operator> child_op, std::vector<std::string>&& column_names, bool descending = false, size_t limit = Constants::ORDER_BY_LIMIT, size_t offset = 0) {
    return std::make_shared<OrderByOperator>(child_op, std::move(column_names), descending, limit, offset);
}

void MakeDataPath(const std::string& data_path) {
    data_stream = std::make_unique<std::ifstream>(data_path, std::ios::binary);
    if (!data_stream->is_open()) {
        throw std::runtime_error("cannot open data file: " + data_path);
    }
}

// SELECT COUNT(*) FROM hits;
std::shared_ptr<Operator> MakeQuery0() {
    auto scan = MakeScan({"AdvEngineID"});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>()};
    return std::make_shared<AggregateOperator>(scan, aggregations);
}

// SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;
std::shared_ptr<Operator> MakeQuery1() {
    auto scan = MakeScan({"AdvEngineID"});
    auto filter = MakeFilter(scan, {"AdvEngineID"}, {"0"}, {CompareSign::NOT_EQUAL});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>()};
    return std::make_shared<AggregateOperator>(filter, aggregations);
}

// SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;
std::shared_ptr<Operator> MakeQuery2() {
    auto scan = MakeScan({"AdvEngineID", "ResolutionWidth"});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<SumAggregation>("AdvEngineID"), std::make_shared<CountAggregation>(), std::make_shared<AvgAggregation>("ResolutionWidth")};
    return std::make_shared<AggregateOperator>(scan, aggregations);
}

// SELECT AVG(UserID) FROM hits;
std::shared_ptr<Operator> MakeQuery3() {
    auto scan = MakeScan({"UserID"});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<AvgAggregation>("UserID")};
    return std::make_shared<AggregateOperator>(scan, aggregations);
}

// SELECT COUNT(DISTINCT UserID) FROM hits;
std::shared_ptr<Operator> MakeQuery4() {
    auto scan = MakeScan({"UserID"});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountDistinctAggregation>("UserID")};
    return std::make_shared<AggregateOperator>(scan, aggregations);
}

// SELECT COUNT(DISTINCT SearchPhrase) FROM hits;
std::shared_ptr<Operator> MakeQuery5() {
    auto scan = MakeScan({"SearchPhrase"});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountDistinctAggregation>("SearchPhrase")};
    return std::make_shared<AggregateOperator>(scan, aggregations);
}

// SELECT MIN(EventDate), MAX(EventDate) FROM hits;
std::shared_ptr<Operator> MakeQuery6() {
    auto scan = MakeScan({"EventDate"});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<MinAggregation>("EventDate"), std::make_shared<MaxAggregation>("EventDate")};
    return std::make_shared<AggregateOperator>(scan, aggregations);
}

// SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID  ORDER BY COUNT(*) DESC;
std::shared_ptr<Operator> MakeQuery7() {
    auto scan = MakeScan({"AdvEngineID"});
    auto filter = MakeFilter(scan, {"AdvEngineID"}, {"0"}, {CompareSign::NOT_EQUAL});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>()};
    auto group_by = MakeGroupBy(filter, {"AdvEngineID"}, aggregations);
    return MakeOrderBy(group_by, {"COUNT(*)"}, true);
}

// SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery8() {
    auto scan = MakeScan({"RegionID", "UserID"});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountDistinctAggregation>("UserID", "u")};
    auto group_by = MakeGroupBy(scan, {"RegionID"}, aggregations);
    return MakeOrderBy(group_by, {"u"}, true, 10);
}

// SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery9() {
    auto scan = MakeScan({"RegionID", "AdvEngineID", "ResolutionWidth", "UserID"});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{
        std::make_shared<SumAggregation>("AdvEngineID"), 
        std::make_shared<CountAggregation>("c"), 
        std::make_shared<AvgAggregation>("ResolutionWidth"), 
        std::make_shared<CountDistinctAggregation>("UserID")};
    auto group_by = MakeGroupBy(scan, {"RegionID"}, aggregations);
    return MakeOrderBy(group_by, {"c"}, true, 10);
}

// SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery10() {
    auto scan = MakeScan({"MobilePhoneModel", "UserID"});
    auto filter = MakeFilter(scan, {"MobilePhoneModel"}, {""}, {CompareSign::NOT_EQUAL});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountDistinctAggregation>("UserID", "u")};
    auto group_by = MakeGroupBy(filter, {"MobilePhoneModel"}, aggregations);
    return MakeOrderBy(group_by, {"u"}, true, 10);
}

// SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery11() {
    auto scan = MakeScan({"MobilePhone", "MobilePhoneModel", "UserID"});
    auto filter = MakeFilter(scan, {"MobilePhoneModel"}, {""}, {CompareSign::NOT_EQUAL});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountDistinctAggregation>("UserID", "u")};
    auto group_by = MakeGroupBy(filter, {"MobilePhone", "MobilePhoneModel"}, aggregations);
    return MakeOrderBy(group_by, {"u"}, true, 10);
}

// SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery12() {
    auto scan = MakeScan({"SearchPhrase"});
    auto filter = MakeFilter(scan, {"SearchPhrase"}, {""}, {CompareSign::NOT_EQUAL});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>("c")};
    auto group_by = MakeGroupBy(filter, {"SearchPhrase"}, aggregations);
    return MakeOrderBy(group_by, {"c"}, true, 10);
}

// SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery13() {
    auto scan = MakeScan({"SearchPhrase", "UserID"});
    auto filter = MakeFilter(scan, {"SearchPhrase"}, {""}, {CompareSign::NOT_EQUAL});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountDistinctAggregation>("UserID", "u")};
    auto group_by = MakeGroupBy(filter, {"SearchPhrase"}, aggregations);
    return MakeOrderBy(group_by, {"u"}, true, 10);
}

// SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery14() {
    auto scan = MakeScan({"SearchEngineID", "SearchPhrase"});
    auto filter = MakeFilter(scan, {"SearchPhrase"}, {""}, {CompareSign::NOT_EQUAL});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>("c")};
    auto group_by = MakeGroupBy(filter, {"SearchEngineID", "SearchPhrase"}, aggregations);
    return MakeOrderBy(group_by, {"c"}, true, 10);
}

// SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery15() {
    auto scan = MakeScan({"UserID"});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>()};
    auto group_by = MakeGroupBy(scan, {"UserID"}, aggregations);
    return MakeOrderBy(group_by, {"COUNT(*)"}, true, 10);
}

// SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery16() {
    auto scan = MakeScan({"UserID", "SearchPhrase"});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>()};
    auto group_by = MakeGroupBy(scan, {"UserID", "SearchPhrase"}, aggregations);
    return MakeOrderBy(group_by, {"COUNT(*)"}, true, 10);
}

// SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;
std::shared_ptr<Operator> MakeQuery17() {
    auto scan = MakeScan({"UserID", "SearchPhrase"});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>()};
    auto group_by = MakeGroupBy(scan, {"UserID", "SearchPhrase"}, aggregations);
    return std::make_shared<LimitOperator>(group_by, 10);
}

// SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery18() {
    auto scan = MakeScan({"UserID", "EventTime", "SearchPhrase"});
    auto transforms = std::vector<std::shared_ptr<Transform>>{std::make_shared<ExtractMinuteTransform>("EventTime", "m")};
    auto transform = std::make_shared<TransformsOperator>(scan, std::move(transforms));
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>()};
    auto group_by = MakeGroupBy(transform, {"UserID", "m", "SearchPhrase"}, aggregations);
    return MakeOrderBy(group_by, {"COUNT(*)"}, true, 10);
}

// SELECT UserID FROM hits WHERE UserID = 435090932899640449;
std::shared_ptr<Operator> MakeQuery19() {
    auto scan = MakeScan({"UserID"});
    auto filter = MakeFilter(scan, {"UserID"}, {"435090932899640449"}, {CompareSign::EQUAL});
    return filter;
}

// SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';
std::shared_ptr<Operator> MakeQuery20() {
    auto scan = MakeScan({"URL"});
    auto filter = MakeFilter(scan, {"URL"}, {"%google%"}, {CompareSign::LIKE});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>()};
    return std::make_shared<AggregateOperator>(filter, aggregations);
}

// SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery21() {
    auto scan = MakeScan({"URL", "SearchPhrase"});
    auto filter = MakeFilter(scan, {"URL", "SearchPhrase"}, {"%google%", ""}, {CompareSign::LIKE, CompareSign::NOT_EQUAL});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<MinAggregation>("URL"), std::make_shared<CountAggregation>("c")};
    auto group_by = MakeGroupBy(filter, {"SearchPhrase"}, aggregations);
    return MakeOrderBy(group_by, {"c"}, true, 10);
}

// SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery22() {
    auto scan = MakeScan({"URL", "Title", "SearchPhrase", "UserID"});
    auto filter = MakeFilter(scan, {"Title", "URL", "SearchPhrase"}, {"%Google%", "%.google.%", ""}, {CompareSign::LIKE, CompareSign::NOT_LIKE, CompareSign::NOT_EQUAL});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{
        std::make_shared<MinAggregation>("URL"), 
        std::make_shared<MinAggregation>("Title"), 
        std::make_shared<CountAggregation>("c"), 
        std::make_shared<CountDistinctAggregation>("UserID")};
    auto group_by = MakeGroupBy(filter, {"SearchPhrase"}, aggregations);
    return MakeOrderBy(group_by, {"c"}, true, 10);
}

// SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;
std::shared_ptr<Operator> MakeQuery23() {
    auto scan = MakeScan({
        "WatchID", "JavaEnable", "Title", "GoodEvent", "EventTime", "EventDate", "CounterID", "ClientIP",
        "RegionID", "UserID", "CounterClass", "OS", "UserAgent", "URL", "Referer", "IsRefresh",
        "RefererCategoryID", "RefererRegionID", "URLCategoryID", "URLRegionID", "ResolutionWidth",
        "ResolutionHeight", "ResolutionDepth", "FlashMajor", "FlashMinor", "FlashMinor2", "NetMajor",
        "NetMinor", "UserAgentMajor", "UserAgentMinor", "CookieEnable", "JavascriptEnable", "IsMobile",
        "MobilePhone", "MobilePhoneModel", "Params", "IPNetworkID", "TraficSourceID", "SearchEngineID",
        "SearchPhrase", "AdvEngineID", "IsArtifical", "WindowClientWidth", "WindowClientHeight",
        "ClientTimeZone", "ClientEventTime", "SilverlightVersion1", "SilverlightVersion2",
        "SilverlightVersion3", "SilverlightVersion4", "PageCharset", "CodeVersion", "IsLink",
        "IsDownload", "IsNotBounce", "FUniqID", "OriginalURL", "HID", "IsOldCounter", "IsEvent",
        "IsParameter", "DontCountHits", "WithHash", "HitColor", "LocalEventTime", "Age", "Sex",
        "Income", "Interests", "Robotness", "RemoteIP", "WindowName", "OpenerName", "HistoryLength",
        "BrowserLanguage", "BrowserCountry", "SocialNetwork", "SocialAction", "HTTPError", "SendTiming",
        "DNSTiming", "ConnectTiming", "ResponseStartTiming", "ResponseEndTiming", "FetchTiming",
        "SocialSourceNetworkID", "SocialSourcePage", "ParamPrice", "ParamOrderID", "ParamCurrency",
        "ParamCurrencyID", "OpenstatServiceName", "OpenstatCampaignID", "OpenstatAdID",
        "OpenstatSourceID", "UTMSource", "UTMMedium", "UTMCampaign", "UTMContent", "UTMTerm",
        "FromTag", "HasGCLID", "RefererHash", "URLHash", "CLID"});
    auto filter = MakeFilter(scan, {"URL"}, {"%google%"}, {CompareSign::LIKE});
    return MakeOrderBy(filter, {"EventTime"}, false, 10);
}

// SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;
std::shared_ptr<Operator> MakeQuery24() {
    auto scan = MakeScan({"SearchPhrase", "EventTime"});
    auto filter = MakeFilter(scan, {"SearchPhrase"}, {""}, {CompareSign::NOT_EQUAL});
    return MakeOrderBy(filter, {"EventTime"}, false, 10);
}

// SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;
std::shared_ptr<Operator> MakeQuery25() {
    auto scan = MakeScan({"SearchPhrase"});
    auto filter = MakeFilter(scan, {"SearchPhrase"}, {""}, {CompareSign::NOT_EQUAL});
    return MakeOrderBy(filter, {"SearchPhrase"}, false, 10);
}

// SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10;
std::shared_ptr<Operator> MakeQuery26() {
    auto scan = MakeScan({"SearchPhrase", "EventTime"});
    auto filter = MakeFilter(scan, {"SearchPhrase"}, {""}, {CompareSign::NOT_EQUAL});
    return MakeOrderBy(filter, {"EventTime", "SearchPhrase"}, false, 10);
}

// SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;
std::shared_ptr<Operator> MakeQuery27() {
    auto scan = MakeScan({"CounterID", "URL"});
    auto filter = MakeFilter(scan, {"URL"}, {""}, {CompareSign::NOT_EQUAL});
    auto transforms = std::vector<std::shared_ptr<Transform>>{std::make_shared<LengthTransform>("URL", "length(URL)")};
    auto transform = std::make_shared<TransformsOperator>(filter, std::move(transforms));
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<AvgAggregation>("length(URL)", "l"), std::make_shared<CountAggregation>("c")};
    auto group_by = MakeGroupBy(transform, {"CounterID"}, aggregations);
    auto having_filter = MakeFilter(group_by, {"c"}, {"100000"}, {CompareSign::GREATER});
    return MakeOrderBy(having_filter, {"l"}, true, 25);
}

// SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;
std::shared_ptr<Operator> MakeQuery28() {
    auto scan = MakeScan({"Referer"});
    auto filter = MakeFilter(scan, {"Referer"}, {""}, {CompareSign::NOT_EQUAL});
    auto transforms = std::vector<std::shared_ptr<Transform>>{std::make_shared<RegexpReplaceTransform>("Referer", "^https?://(?:www\\.)?([^/]+)/.*$", "$1", "k"), std::make_shared<LengthTransform>("Referer", "length(Referer)")};
    auto transform = std::make_shared<TransformsOperator>(filter, std::move(transforms));
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<AvgAggregation>("length(Referer)", "l"), std::make_shared<CountAggregation>("c"), std::make_shared<MinAggregation>("Referer")};
    auto group_by = MakeGroupBy(transform, {"k"}, aggregations);
    auto having_filter = MakeFilter(group_by, {"c"}, {"100000"}, {CompareSign::GREATER});
    return MakeOrderBy(having_filter, {"l"}, true, 25);
}

// SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), SUM(ResolutionWidth + 4), SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), SUM(ResolutionWidth + 15), SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), SUM(ResolutionWidth + 24), SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), SUM(ResolutionWidth + 35), SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), SUM(ResolutionWidth + 44), SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), SUM(ResolutionWidth + 55), SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), SUM(ResolutionWidth + 64), SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), SUM(ResolutionWidth + 75), SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), SUM(ResolutionWidth + 84), SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) FROM hits;
std::shared_ptr<Operator> MakeQuery29() {
    auto scan = MakeScan({"ResolutionWidth"});
    std::vector<std::shared_ptr<Transform>> transforms;
    for (size_t i = 1; i <= 89; ++i) {
        transforms.push_back(std::make_shared<AddTransform>("ResolutionWidth", i));
    }
    auto transform = std::make_shared<TransformsOperator>(scan, std::move(transforms));
    std::vector<std::shared_ptr<Aggregation>> aggregations;
    for (size_t i = 0; i <= 89; ++i) {
        const std::string column_name = i == 0 ? "ResolutionWidth" : "ResolutionWidth + " + std::to_string(i);
        aggregations.push_back(std::make_shared<SumAggregation>(column_name));
    }
    return std::make_shared<AggregateOperator>(transform, aggregations);
}

// SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery30() {
    auto scan = MakeScan({"SearchEngineID", "ClientIP", "IsRefresh", "ResolutionWidth", "SearchPhrase"});
    auto filter = MakeFilter(scan, {"SearchPhrase"}, {""}, {CompareSign::NOT_EQUAL});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{
        std::make_shared<CountAggregation>("c"), 
        std::make_shared<SumAggregation>("IsRefresh"), 
        std::make_shared<AvgAggregation>("ResolutionWidth")};
    auto group_by = MakeGroupBy(filter, {"SearchEngineID", "ClientIP"}, aggregations);
    return MakeOrderBy(group_by, {"c"}, true, 10);
}

// SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery31() {
    auto scan = MakeScan({"WatchID", "ClientIP", "IsRefresh", "ResolutionWidth", "SearchPhrase"});
    auto filter = MakeFilter(scan, {"SearchPhrase"}, {""}, {CompareSign::NOT_EQUAL});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{
        std::make_shared<CountAggregation>("c"), 
        std::make_shared<SumAggregation>("IsRefresh"), 
        std::make_shared<AvgAggregation>("ResolutionWidth")};
    auto group_by = MakeGroupBy(filter, {"WatchID", "ClientIP"}, aggregations);
    return MakeOrderBy(group_by, {"c"}, true, 10);
}

// SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery32() {
    auto scan = MakeScan({"WatchID", "ClientIP", "IsRefresh", "ResolutionWidth"});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{
        std::make_shared<CountAggregation>("c"), 
        std::make_shared<SumAggregation>("IsRefresh"), 
        std::make_shared<AvgAggregation>("ResolutionWidth")};
    auto group_by = MakeGroupBy(scan, {"WatchID", "ClientIP"}, aggregations);
    return MakeOrderBy(group_by, {"c"}, true, 10);
}

// SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery33() {
    auto scan = MakeScan({"URL"});
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>("c")};
    auto group_by = MakeGroupBy(scan, {"URL"}, aggregations);
    return MakeOrderBy(group_by, {"c"}, true, 10);
}

// SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery34() {
    auto scan = MakeScan({"URL"});
    auto transforms = std::vector<std::shared_ptr<Transform>>{std::make_shared<ConstantTransform>("1", "1")};
    auto transform = std::make_shared<TransformsOperator>(scan, std::move(transforms));
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>("c")};
    auto group_by = MakeGroupBy(transform, {"1", "URL"}, aggregations);
    return MakeOrderBy(group_by, {"c"}, true, 10);
}

// SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery35() {
    auto scan = MakeScan({"ClientIP"});
    std::vector<std::shared_ptr<Transform>> transforms;
    for (size_t i = 1; i <= 3; ++i) {
        transforms.push_back(std::make_shared<SubTransform>("ClientIP", i));
    }
    auto transform = std::make_shared<TransformsOperator>(scan, std::move(transforms));
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>("c")};
    auto group_by = MakeGroupBy(transform, {"ClientIP", "ClientIP - 1", "ClientIP - 2", "ClientIP - 3"}, aggregations);
    return MakeOrderBy(group_by, {"c"}, true, 10);
}

// SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery36() {
    auto scan = MakeScan({"URL", "CounterID", "EventDate", "DontCountHits", "IsRefresh"});
    auto filter = MakeFilter(
        scan,
        {"CounterID", "EventDate", "EventDate", "DontCountHits", "IsRefresh", "URL"},
        {"62", "2013-07-01", "2013-07-31", "0", "0", ""},
        {CompareSign::EQUAL, CompareSign::GREATER_OR_EQUAL, CompareSign::LESS_OR_EQUAL, CompareSign::EQUAL, CompareSign::EQUAL, CompareSign::NOT_EQUAL}
    );
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>("PageViews")};
    auto group_by = MakeGroupBy(filter, {"URL"}, aggregations);
    return MakeOrderBy(group_by, {"PageViews"}, true, 10);
}

// SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10;
std::shared_ptr<Operator> MakeQuery37() {
    auto scan = MakeScan({"Title", "CounterID", "EventDate", "DontCountHits", "IsRefresh"});
    auto filter = MakeFilter(
        scan,
        {"CounterID", "EventDate", "EventDate", "DontCountHits", "IsRefresh", "Title"},
        {"62", "2013-07-01", "2013-07-31", "0", "0", ""},
        {CompareSign::EQUAL, CompareSign::GREATER_OR_EQUAL, CompareSign::LESS_OR_EQUAL, CompareSign::EQUAL, CompareSign::EQUAL, CompareSign::NOT_EQUAL}
    );
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>("PageViews")};
    auto group_by = MakeGroupBy(filter, {"Title"}, aggregations);
    return MakeOrderBy(group_by, {"PageViews"}, true, 10);
}

// SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;
std::shared_ptr<Operator> MakeQuery38() {
    auto scan = MakeScan({"URL", "CounterID", "EventDate", "IsRefresh", "IsLink", "IsDownload"});
    auto filter = MakeFilter(
        scan,
        {"CounterID", "EventDate", "EventDate", "IsRefresh", "IsLink", "IsDownload"},
        {"62", "2013-07-01", "2013-07-31", "0", "0", "0"},
        {CompareSign::EQUAL, CompareSign::GREATER_OR_EQUAL, CompareSign::LESS_OR_EQUAL, CompareSign::EQUAL, CompareSign::NOT_EQUAL, CompareSign::EQUAL}
    );
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>("PageViews")};
    auto group_by = MakeGroupBy(filter, {"URL"}, aggregations);
    return MakeOrderBy(group_by, {"PageViews"}, true, 10, 1000);
}

// SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;
std::shared_ptr<Operator> MakeQuery39() {
    auto scan = MakeScan({"TraficSourceID", "SearchEngineID", "AdvEngineID", "Referer", "URL", "CounterID", "EventDate", "IsRefresh"});
    auto filter = MakeFilter(
        scan,
        {"CounterID", "EventDate", "EventDate", "IsRefresh"},
        {"62", "2013-07-01", "2013-07-31", "0"},
        {CompareSign::EQUAL, CompareSign::GREATER_OR_EQUAL, CompareSign::LESS_OR_EQUAL, CompareSign::EQUAL}
    );
    auto transforms = std::vector<std::shared_ptr<Transform>>{
        std::make_shared<CaseWhenTransform>(
            std::vector<std::string>{"SearchEngineID", "AdvEngineID"},
            std::vector<std::string>{"0", "0"},
            std::vector<CompareSign>{CompareSign::EQUAL, CompareSign::EQUAL},
            "Referer",
            "",
            "Src"
        ),
        std::make_shared<RenameTransform>("URL", "Dst")
    };
    auto transform = std::make_shared<TransformsOperator>(filter, std::move(transforms));
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>("PageViews")};
    auto group_by = MakeGroupBy(transform, {"TraficSourceID", "SearchEngineID", "AdvEngineID", "Src", "Dst"}, aggregations);
    return MakeOrderBy(group_by, {"PageViews"}, true, 10, 1000);
}

// SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100;
std::shared_ptr<Operator> MakeQuery40() {
    auto scan = MakeScan({"URLHash", "EventDate", "CounterID", "IsRefresh", "TraficSourceID", "RefererHash"});
    auto filter = MakeFilter(
        scan,
        {"CounterID", "EventDate", "EventDate", "IsRefresh", "TraficSourceID", "RefererHash"},
        {"62", "2013-07-01", "2013-07-31", "0", "-1,6", "3594120000172545465"},
        {CompareSign::EQUAL, CompareSign::GREATER_OR_EQUAL, CompareSign::LESS_OR_EQUAL, CompareSign::EQUAL, CompareSign::IN, CompareSign::EQUAL}
    );
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>("PageViews")};
    auto group_by = MakeGroupBy(filter, {"URLHash", "EventDate"}, aggregations);
    return MakeOrderBy(group_by, {"PageViews"}, true, 10, 100);
}

// SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000;
std::shared_ptr<Operator> MakeQuery41() {
    auto scan = MakeScan({"WindowClientWidth", "WindowClientHeight", "CounterID", "EventDate", "IsRefresh", "DontCountHits", "URLHash"});
    auto filter = MakeFilter(
        scan,
        {"CounterID", "EventDate", "EventDate", "IsRefresh", "DontCountHits", "URLHash"},
        {"62", "2013-07-01", "2013-07-31", "0", "0", "2868770270353813622"},
        {CompareSign::EQUAL, CompareSign::GREATER_OR_EQUAL, CompareSign::LESS_OR_EQUAL, CompareSign::EQUAL, CompareSign::EQUAL, CompareSign::EQUAL}
    );
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>("PageViews")};
    auto group_by = MakeGroupBy(filter, {"WindowClientWidth", "WindowClientHeight"}, aggregations);
    return MakeOrderBy(group_by, {"PageViews"}, true, 10, 10000);
}

// SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000;
std::shared_ptr<Operator> MakeQuery42() {
    auto scan = MakeScan({"EventTime", "CounterID", "EventDate", "IsRefresh", "DontCountHits"});
    auto filter = MakeFilter(
        scan,
        {"CounterID", "EventDate", "EventDate", "IsRefresh", "DontCountHits"},
        {"62", "2013-07-14", "2013-07-15", "0", "0"},
        {CompareSign::EQUAL, CompareSign::GREATER_OR_EQUAL, CompareSign::LESS_OR_EQUAL, CompareSign::EQUAL, CompareSign::EQUAL}
    );
    auto transforms = std::vector<std::shared_ptr<Transform>>{
        std::make_shared<DateTruncMinuteTransform>("EventTime", "M")
    };
    auto transform = std::make_shared<TransformsOperator>(filter, std::move(transforms));
    auto aggregations = std::vector<std::shared_ptr<Aggregation>>{std::make_shared<CountAggregation>("PageViews")};
    auto group_by = MakeGroupBy(transform, {"M"}, aggregations);
    return MakeOrderBy(group_by, {"M"}, false, 10, 1000);
}

size_t ParseQueryNumber(const char* value) {
    const std::string query_text = value;
    size_t parsed_length = 0;
    const unsigned long parsed_value = std::stoul(query_text, &parsed_length);
    if (parsed_length != query_text.size()) {
        throw std::runtime_error("query number must be an integer");
    }
    return static_cast<size_t>(parsed_value);
}

std::shared_ptr<Operator> BuildQuery(size_t query_num) {
    static std::shared_ptr<Operator> (*const query_builders[])() = {
        MakeQuery0, MakeQuery1, MakeQuery2, MakeQuery3, MakeQuery4, MakeQuery5, MakeQuery6,
        MakeQuery7, MakeQuery8, MakeQuery9, MakeQuery10, MakeQuery11, MakeQuery12, MakeQuery13,
        MakeQuery14, MakeQuery15, MakeQuery16, MakeQuery17, MakeQuery18, MakeQuery19, MakeQuery20,
        MakeQuery21, MakeQuery22, MakeQuery23, MakeQuery24, MakeQuery25, MakeQuery26, MakeQuery27,
        MakeQuery28, MakeQuery29, MakeQuery30, MakeQuery31, MakeQuery32, MakeQuery33, MakeQuery34,
        MakeQuery35, MakeQuery36, MakeQuery37, MakeQuery38, MakeQuery39, MakeQuery40, MakeQuery41,
        MakeQuery42
    };

    constexpr size_t query_count = sizeof(query_builders) / sizeof(query_builders[0]);
    if (query_num >= query_count) {
        throw std::runtime_error("query number is out of range [0, 42]");
    }

    return query_builders[query_num]();
}

void RunQuery(size_t query_num, const std::string& input_path, const std::string& output_path) {
    MakeDataPath(input_path);

    std::ofstream result_stream(output_path);
    if (!result_stream.is_open()) {
        throw std::runtime_error("cannot open output file: " + output_path);
    }

    const auto start_time = std::chrono::steady_clock::now();
    auto executor = ExecuteOperator(BuildQuery(query_num));
    Writer result_writer(result_stream);
    while (auto batch = executor->NextBatch()) {
        batch_serialization::WriteCsvBatch(*batch, result_writer);
    }
    const auto end_time = std::chrono::steady_clock::now();
    const std::chrono::duration<double> elapsed = end_time - start_time;
    std::cerr << "Query " << query_num << " took " << elapsed.count() << " seconds\n";
}

int main(int argc, char** argv) {
    try {
        if (argc < 2) {
            Helper(argv[0]);
            return 1;
        }

        const size_t query_num = ParseQueryNumber(argv[1]);
        EnsureNoExtraArguments(argc, 4);
        RunQuery(
            query_num,
            GetArgument(argc, argv, 2, default_mf_input_path),
            GetArgument(argc, argv, 3, GetDefaultResultPath(query_num))
        );
        return 0;
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
