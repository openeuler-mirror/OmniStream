/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: StreamOneInputProcess for DataStream
 */
#ifndef FLINK_TNEL_TYPEINFOFACTORY_H
#define FLINK_TNEL_TYPEINFOFACTORY_H

#include <string>
#include <nlohmann/json.hpp>
#include "TypeInformation.h"
#include "table/types/logical/LogicalType.h"
#include "types/logical/RowType.h"

// for convenience
using json = nlohmann::json;
using string = std::string;

class TypeInfoFactory {
public:
    static TypeInformation *createTypeInfo(const char* name, const std::string config);

    // internalType  is mapped from java table internal type, which is for sql type
    static TypeInformation *createInternalTypeInfo(const nlohmann::json &rowType);

    // Tuple
    static TypeInformation *createTupleTypeInfo(const nlohmann::json &rowType);

    static TypeInformation *createCommittableMessageInfo();

    // may not be  useful, leave it as it for now. basic internal type is mapped from java sql type, logicaltyperoot.PREDEFINED,
    static TypeInformation *createBasicInternalTypeInfo(const char* name, const std::string config);

    static LogicalType* createLogicalType(const std::string type);

    static RowType* createRowType(const std::vector<omniruntime::type::DataTypeId> *inputRowType);
};


#endif   //FLINK_TNEL_TYPEINFOFACTORY_H
