#ifndef STREAMPARTITIONER_V2_H
#define STREAMPARTITIONER_V2_H

#include <string>
#include <arm_sve.h>
#include "ChannelSelectorV2.h"
#include "SubtaskStateMapper.h"
#include "streamrecord/StreamRecord.h"
#include "table/vectorbatch/VectorBatch.h"
#include "vector/vector_helper.h"
#include "Row.h"
#include <arm_sve.h>

namespace omnistream {
template<typename T>
class StreamPartitionerV2 : public ChannelSelectorV2<T> {
public:
    StreamPartitionerV2() : numberOfChannels(0) {
    }

    virtual ~StreamPartitionerV2() {
    }

    void setup(int numberOfChannels) override {
        this->numberOfChannels = numberOfChannels;
    }

    [[nodiscard]] bool isBroadcast() const override {
        return false;
    }
    virtual int selectRowChannel(omnistream::VectorBatch* record, int rowIndex) = 0;

    virtual int selectRowChannel(RowData* record) = 0;

    std::unordered_map<int, T*> selectChannel(T* record) override {
        std::unordered_map<int, T*> result;
        std::unordered_map<int, std::vector<int32_t>> intermediateResult;
        auto streamRecord = reinterpret_cast<StreamRecord*>(record);
        if (streamRecord->hasExternalRow()) {
            auto row = static_cast<Row*>(streamRecord->getValue());
            long timestamp = streamRecord->getTimestamp();

            auto channel = selectRowChannel(row->getInternalRow());
            intermediateResult[channel].push_back(0);

            for (auto& pair : intermediateResult) {
                result[pair.first] = buildNewStreamRecordBasedOnOffsets(streamRecord, timestamp);
            }

            delete row->getInternalRow();
            delete row;
            delete streamRecord;
            return result;
        }
        auto vectorBatch = static_cast<VectorBatch*>(streamRecord->getValue());

        long timestamp = streamRecord->getTimestamp();

        size_t row_cnt = vectorBatch->GetRowCount();

        for (size_t i = 0; i < row_cnt; i++) {
            // auto rowData = vectorBatch->extractRowData(i);
            auto channel = selectRowChannel(vectorBatch, i);
            intermediateResult[channel].push_back(i);
        }

        // convert intermediateResult to result
        for (auto& pair : intermediateResult) {
            result[pair.first] = buildNewStreamRecordBasedOnOffsets(pair.second, streamRecord, timestamp);
        }

        delete vectorBatch;
//                delete streamRecord;

        return result;
    }

    virtual std::unique_ptr<StreamPartitionerV2<T>> copy() = 0;
    virtual std::unique_ptr<SubtaskStateMapper> getDownstreamSubtaskStateMapper() = 0;
    virtual bool isPointwise() = 0;
    [[nodiscard]] virtual std::string toString() const = 0;
    virtual std::unique_ptr<SubtaskStateMapper> getUpstreamSubtaskStateMapper() = 0;

    void setRowKind_sve(int i, int size, uint8_t* src, int32_t* offsets, uint8_t* dst) {
        auto pg = svwhilelt_b32(i, size);
        svint32_t offsetRaw = svld1_s32(pg, offsets);
        svuint32_t rawData = svld1ub_gather_offset_u32(pg, src, offsetRaw);

        svuint8_t u8_vec = svreinterpret_u8_u32(rawData);
        svuint8_t indices = svindex_u8(0, sizeof(uint32_t));
        svuint8_t packed = svtbl(u8_vec, indices);

        auto pg2 = svwhilelt_b8(i, size);
        svst1_u8(pg2, dst, packed);
    }

    void setTimestamp_sve(int i, int size, int64_t* src, int32_t* offsets, int64_t* dst) {
        auto pg = svwhilelt_b32(i, size);
        svint32_t offsetRaw = svld1_s32(pg, offsets);
        svint64_t offset1 = svunpklo(offsetRaw);
        svint64_t offset2 = svunpkhi(offsetRaw);

        auto pg2 = svwhilelt_b64(i, size);
        svint64_t rawData = svld1_gather_index(pg2, src, offset1);
        svst1_s64(pg2, dst, rawData);

        int jump = 4;
        auto pg3 = svwhilelt_b64(i + jump, size);
        svint64_t rawData2 = svld1_gather_index(pg3, src, offset2);
        svst1_s64(pg3, dst + jump, rawData2);
    }

    StreamRecord* buildNewStreamRecordBasedOnOffsets(std::vector<int>& offsets, StreamRecord* originStreamRecord,
         long timestamp)
    {
        auto vectorBatch = reinterpret_cast<VectorBatch*>(originStreamRecord->getValue());
        auto copyedVectorBatch = new VectorBatch(offsets.size());
        int32_t vectorCount = vectorBatch->GetVectorCount();
        for (int i = 0; i < vectorCount; i++) {
            // The original vector is not varchar or it is varchar but flat
            if (vectorBatch->Get(i)->GetEncoding() == omniruntime::vec::OMNI_FLAT ||
                (vectorBatch->Get(i)->GetTypeId() != omniruntime::type::OMNI_CHAR
                && vectorBatch->Get(i)->GetTypeId() != omniruntime::type::OMNI_VARCHAR)) {
                copyedVectorBatch->Append(omniruntime::vec::VectorHelper::CopyPositionsVector(
                    vectorBatch->Get(i), offsets.data(), 0, offsets.size()));
            } else {
                // It is a varchar dictionary, copy it out
                copyedVectorBatch->Append(omnistream::VectorBatch::CopyPositionsAndFlatten(vectorBatch->Get(i),
                    offsets.data(), 0, offsets.size()));
            }
        }
        int processElement = 8;
        int size = offsets.size();
        for (size_t i = 0; i < offsets.size(); i += processElement) {
            int position = offsets[i];

            setRowKind_sve(i, size, reinterpret_cast<uint8_t*>(vectorBatch->getRowKinds()), offsets.data() + i, reinterpret_cast<uint8_t*>(copyedVectorBatch->getRowKinds()) + i);
            setTimestamp_sve(i, size, vectorBatch->getTimestamps(), offsets.data() + i, copyedVectorBatch->getTimestamps() + i);
        }
        return new StreamRecord(copyedVectorBatch, timestamp);
}

StreamRecord* buildNewStreamRecordBasedOnOffsets(StreamRecord* originStreamRecord, long timestamp) {
    auto row = reinterpret_cast<Row*>(originStreamRecord->getValue());
    auto copyRow = new Row(row->getKind(), row->getFieldByPosition(),
        row->getFieldByName(), row->getPositionByName());
    return new StreamRecord(copyRow, timestamp);
}
protected:
    int numberOfChannels;
};
}


#endif
