#pragma once

#include "engine/data_storage/batch.h"
#include <memory>
#include <queries_executor/operator.h>

class PipelineExecutor {
public:
    virtual ~PipelineExecutor() = default;
    virtual std::shared_ptr<Batch> NextBatch() = 0;
private:
};

std::shared_ptr<PipelineExecutor> ExecuteOperator(std::shared_ptr<Operator> op);
