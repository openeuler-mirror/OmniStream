
#ifndef FLINK_TNEL_ROWDATASERIALIZER_H
#define FLINK_TNEL_ROWDATASERIALIZER_H

#include <vector>

#include "../../core/typeutils/TypeSerializerSingleton.h"
#include "BinaryRowDataSerializer.h"
#include "../types/logical/LogicalType.h"
#include "../types/logical/RowType.h"
#include "../data/binary/BinaryRowData.h"
#include "../data/writer/BinaryRowWriter.h"


class RowDataSerializer : public TypeSerializerSingleton  {
public:
    //RowDataSerializer(std::vector<LogicalType*> * types, std::vector<TypeSerializer *> *fieldSerializers);

    explicit RowDataSerializer(RowType *rowType);
    ~RowDataSerializer();

    void *deserialize(DataInputView &source) override;

    void serialize(void *record, DataOutputSerializer &target) override;

    [[nodiscard]] const char *getName() const override;

    BinaryRowData * toBinaryRow(RowData* row);

    BackendDataType getBackendId() const override { return BackendDataType::ROW_BK;};
    
private:
    BinaryRowDataSerializer binarySerializer_;
    std::vector<LogicalType *> types_;
    std::vector<TypeSerializer*> fieldSerializers_;
    std::vector<FieldGetter*> fieldGetters_;

     BinaryRowData* reuseRow_;
     BinaryRowWriter* reuseWriter_;
};


#endif //FLINK_TNEL_ROWDATASERIALIZER_H
