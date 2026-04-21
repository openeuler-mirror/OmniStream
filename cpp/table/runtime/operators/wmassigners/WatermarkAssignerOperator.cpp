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
#include "WatermarkAssignerOperator.h"

WatermarkAssignerOperator::WatermarkAssignerOperator(
    Output *output, int rowtimeIndex, int64_t outOfOrderT, int64_t idleTimeout, ProcessingTimeService* processingTimeService)
    : rowtimeIndex_(rowtimeIndex), outOfOrderTime_(outOfOrderT), idleTimeout_(idleTimeout)
{
    setOutput(output);
    currentStatus = new WatermarkStatus(WatermarkStatus::activeStatus);
    lastWatermark_ = 0 - outOfOrderT;
    setProcessingTimeService(processingTimeService);
}

omnistream::VectorBatch *WatermarkAssignerOperator::sliceVecBatch(omnistream::VectorBatch *batch, int32_t offset, int32_t newRowCnt){
    if(newRowCnt == 0 || newRowCnt == INT64_MAX){
        LOG("Warning: split batch count is not valid.")
        return nullptr;
    }
    std::vector<long> timestamps(newRowCnt);
    std::vector<RowKind> rowkinds(newRowCnt);
    auto oldtimes = batch->getTimestamps();
    auto oldkinds = batch->getRowKinds();
    omnistream::VectorBatch *pBatch = new omnistream::VectorBatch(newRowCnt);
    std::vector<int> offsets(batch->GetRowCount());
    std::iota(offsets.begin(), offsets.end(), 0);
    for (int j = 0; j < newRowCnt; j++){
        timestamps[j] = oldtimes[j + offset];
        rowkinds[j] = oldkinds[j + offset];
    }
    for (int k = 0; k < batch->GetVectorCount(); k++){
        pBatch->Append(omniruntime::vec::VectorHelper::CopyPositionsVector(batch->Get(k), offsets.data(), offset, newRowCnt));
    }
    pBatch->setTimestamps(0, timestamps.data(), newRowCnt);
    pBatch->setRowKinds(0, rowkinds.data(), newRowCnt);
    return pBatch;
}

void WatermarkAssignerOperator::processBatch(StreamRecord *element)
{
    LOG("WaterMark process Batch")

    if (idleTimeout_ > 0 && currentStatus->Equals(WatermarkStatus::idleStatus)) {
        emitWatermarkStatus(new WatermarkStatus(WatermarkStatus::activeStatus));
        lastRecordTime_ = getProcessingTimeService()->getCurrentProcessingTime();
    }

    omnistream::VectorBatch *batch = reinterpret_cast<omnistream::VectorBatch *>(element->getValue());
    bool splitBatch = false;
    int32_t offset = 0;

    auto timeColumn = reinterpret_cast<omniruntime::vec::Vector<int64_t> *>(batch->Get(rowtimeIndex_));

    for (int i = 0; i < timeColumn->GetSize(); i++) {
        auto watermark = timeColumn->GetValue(i);
        currentWatermark_  = std::max(currentWatermark_, watermark - outOfOrderTime_ );
        if (currentWatermark_ - lastWatermark_ > emissionInterval_){
            splitBatch = true;
            int32_t newRowCnt = i + 1 - offset;
            omnistream::VectorBatch *pBatch = sliceVecBatch(batch, offset, newRowCnt);
            output->collect(new StreamRecord(pBatch));
            offset = i + 1;
            advanceWatermark();
        }
    }
    if (splitBatch){
        int32_t newRowCnt = timeColumn->GetSize()-offset;
        if (newRowCnt > 0){
            omnistream::VectorBatch *pBatch = sliceVecBatch(batch, offset, newRowCnt);
            output->collect(new StreamRecord(pBatch));
        }
        delete element;
    }else{
        LOG("no watermark emit, send the original batch")
        output->collect(element);
    }
}

void WatermarkAssignerOperator::processElement(StreamRecord *element)
{
    LOG("WaterMark process element")
    if (idleTimeout_ > 0 && currentStatus->Equals(WatermarkStatus::idleStatus)) {
        emitWatermarkStatus(new WatermarkStatus(WatermarkStatus::activeStatus));
        lastRecordTime_ = getProcessingTimeService()->getCurrentProcessingTime();
    }

    BinaryRowData *row = reinterpret_cast<BinaryRowData *>(element->getValue());

    if (row->isNullAt(rowtimeIndex_)) {
        throw std::logic_error("RowTime field should not be null, please convert it to a non-null long value.");
    }
    // Assume that we get value instead of pointer here, to change later depending on type implementation
    currentWatermark_ = currentWatermark(static_cast<int64_t>(*(row->getLong(rowtimeIndex_))));

    output->collect(element);

    if (currentWatermark_ - lastWatermark_ > emissionInterval_) {
        advanceWatermark();
    }
}

void WatermarkAssignerOperator::advanceWatermark()
{
    if (currentWatermark_ > lastWatermark_) {
        lastWatermark_ = currentWatermark_;
        Watermark *watermark = new Watermark(currentWatermark_);
        output->emitWatermark(watermark);
    }
}

void WatermarkAssignerOperator::emitWatermarkStatus(WatermarkStatus *watermarkStatus)
{
    currentStatus = watermarkStatus;
    output->emitWatermarkStatus(watermarkStatus);
}

void WatermarkAssignerOperator::OnProcessingTime(int64_t timestamp)
{
    advanceWatermark();

    if (idleTimeout_ > 0 && currentStatus->Equals(WatermarkStatus::idleStatus)) {
        const long currentTime = getProcessingTimeService()->getCurrentProcessingTime();
        if (currentTime - lastRecordTime_ > idleTimeout_) {
            emitWatermarkStatus(new WatermarkStatus(WatermarkStatus::activeStatus));
        }
    }

    int64_t now = getProcessingTimeService()->getCurrentProcessingTime();
    getProcessingTimeService()->registerTimer(now + emissionInterval_, this);
}

int64_t WatermarkAssignerOperator::currentWatermark(int64_t element_watermark)
{
    return element_watermark - outOfOrderTime_;
}

void WatermarkAssignerOperator::open()
{
    lastRecordTime_ = getProcessingTimeService()->getCurrentProcessingTime();

    // Set emissionInterval_?
    // Usually set as default value, haven't seen a case where anything other than a predetermined value is used
    if (emissionInterval_ > 0) {
        int64_t now = getProcessingTimeService()->getCurrentProcessingTime();
        getProcessingTimeService()->registerTimer(now + emissionInterval_, this);
    }
}

void WatermarkAssignerOperator::ProcessWatermark(Watermark *mark)
{
    // when we process a watermark with max int64_t limit, it signals end of input
    // and stops the rest of the watermarks to be emmitted
    if (mark->getTimestamp() == INT64_MAX && currentWatermark_ != INT64_MAX) {
        if (idleTimeout_ > 0 && currentStatus->Equals(WatermarkStatus::idleStatus)) {
            // mark the channel active
            emitWatermarkStatus(new WatermarkStatus(WatermarkStatus::activeStatus));
        }
        currentWatermark_ = INT64_MAX;
        output->emitWatermark(mark);
    }
}

void WatermarkAssignerOperator::finish()
{
    ProcessWatermark(new Watermark(INT64_MAX));
}

void WatermarkAssignerOperator::close()
{
}

const char *WatermarkAssignerOperator::getName()
{
    return "SteamWatermarkAssigner";
}

std::string WatermarkAssignerOperator::getTypeName()
{
    return "SteamWatermarkAssigner";
}

void WatermarkAssignerOperator::processWatermarkStatus(WatermarkStatus *watermarkStatus)
{
    emitWatermarkStatus(watermarkStatus);
}
