#ifndef FLINK_TNEL_STRINGDATASERIALIZER_H
#define FLINK_TNEL_STRINGDATASERIALIZER_H

#include <memory>
#include "../../core/typeutils/TypeSerializerSingleton.h"
#include "../data/StringData.h"
#include "OmniOperatorJIT/core/src/type/data_type.h"

using namespace omniruntime::type;

class StringDataSerializer : public TypeSerializerSingleton {
public:
    StringDataSerializer() {};
    void *deserialize(DataInputView &source) override;

    void serialize(void *record, DataOutputSerializer &target) override;

    static StringDataSerializer* INSTANCE;

    BackendDataType getBackendId() const override {
        return BackendDataType::VARCHAR_BK;
    }
};



#endif //FLINK_TNEL_STRINGDATASERIALIZER_H
