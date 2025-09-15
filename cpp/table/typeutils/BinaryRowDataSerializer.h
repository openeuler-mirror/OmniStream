#ifndef FLINK_TNEL_BINARYROWDATASERIALIZER_H
#define FLINK_TNEL_BINARYROWDATASERIALIZER_H

#include "table/data/binary/BinaryRowData.h"
#include "core/typeutils/TypeSerializerSingleton.h"
#include "table/data/utils/JoinedRowData.h"

class BinaryRowDataSerializer : public TypeSerializerSingleton {
public:
    explicit BinaryRowDataSerializer(int numFields);
    ~BinaryRowDataSerializer();

    // void * BinaryRowData
    void *deserialize(DataInputView &source) override;

    void serialize(void *row, DataOutputSerializer &target) override;

    [[nodiscard]] const char *getName() const override;

    static BinaryRowData* joinedRowToBinaryRow(JoinedRowData *row, const std::vector<int32_t>& typeId = {});

    static BinaryRowData* joinedRowFromBothBinaryRow(JoinedRowData *row);

    BackendDataType getBackendId() const override { return BackendDataType::ROW_BK;};
private:
    // TODO
// Add JoinedRowDataSerializer, then pass the unconverted JoinedRowData to
// output collector instead of the converted BinaryRowData

    int numFields_;
    int fixedLengthPartSize_;
    BinaryRowData * reUse_;

    const static int SEG_SIZE = 2048;
    // MemorySegment ** backData_;
    // int numSegment_;

};




#endif //FLINK_TNEL_BINARYROWDATASERIALIZER_H
