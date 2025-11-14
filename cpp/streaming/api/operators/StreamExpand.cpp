/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "StreamExpand.h"
#include "StreamCalc.h"

StreamExpand::StreamExpand(const nlohmann::json &description, Output *output) : description_(description)
{
    this->setOutput(output);
    LOG("StreamExpand description: " << description)
}

StreamExpand::~StreamExpand() = default;

void StreamExpand::open() {
}

void StreamExpand::close() {
}

omnistream::VectorBatch *copyVectorBatch(omnistream::VectorBatch *srcVectorBatch)
{
    int32_t rowCount = srcVectorBatch->GetRowCount();
    omnistream::VectorBatch *pBatch = new omnistream::VectorBatch(rowCount);
    int *pos = new int[rowCount];
    // Positions to copy, AKA all positions
    for (int i = 0; i < rowCount; i++) {
        pos[i] = i;
    }
    for (int index = 0; index < srcVectorBatch->GetVectorCount(); index++) {
        vec::BaseVector *dstVec = VectorHelper::CopyPositionsVector(srcVectorBatch->Get(index), pos, 0, rowCount);
        pBatch->Append(dstVec);
    }
    pBatch->setTimestamps(0, srcVectorBatch->getTimestamps(), rowCount);
    pBatch->setRowKinds(0, srcVectorBatch->getRowKinds(), rowCount);

    return pBatch;
}


void StreamExpand::processBatch(StreamRecord* input)
{
    auto record = reinterpret_cast<omnistream::VectorBatch*>(input->getValue());
    if (description_.contains("projects")) {
        std::vector<nlohmann::json> projections = description_["projects"].get<std::vector<nlohmann::json>>();
        StreamCalcBatch *calcOperator;
        for (nlohmann::json json: projections) {
            // CalcBatch will free input record after process, so expand need copy record to give next Calc.
            omnistream::VectorBatch *record_copy = copyVectorBatch(record);
            calcOperator = new StreamCalcBatch(json, this->output);
            calcOperator->open();
            calcOperator->processBatch(new StreamRecord(record_copy));
            calcOperator->close();
        }
        delete record;
    }
}

const char *StreamExpand::getName()
{
    return OneInputStreamOperator::getName();
}

