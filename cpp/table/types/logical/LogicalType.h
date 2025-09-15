#ifndef FLINK_TNEL_LOGICALTYPE_H
#define FLINK_TNEL_LOGICALTYPE_H

#include <vector>
#include <memory>
#include <unordered_map>
#include <string>
#include "OmniOperatorJIT/core/src/type/data_type.h"

class LogicalType {
protected:
    bool isNullable_;
    int typeId_;

public:
    LogicalType(int typeId, bool isNullable);
    int getTypeId() const;

    bool isNullable() const;
    virtual std::vector<LogicalType*> getChildren() = 0;

    static omniruntime::type::DataTypeId flinkTypeToOmniTypeId(const std::string& flinkType);

    static std::unordered_map<std::string, omniruntime::type::DataTypeId> nameToIdMap;
    static void buildNameToIdMap();
};

class BasicLogicalType : public LogicalType {
private:
    std::vector<LogicalType*> emptyChildren;

public:
    BasicLogicalType(int typeId, bool isNullable) : LogicalType(typeId, isNullable){};

    //Basic type has no children
    std::vector<LogicalType*> getChildren()
    {
        return emptyChildren;
    }
    static BasicLogicalType* BOOLEAN;
    static BasicLogicalType* INTEGER;
    static BasicLogicalType* BIGINT;
    static BasicLogicalType* VARCHAR;
    static BasicLogicalType* TIMESTAMP_WITHOUT_TIME_ZONE;
    static BasicLogicalType* TIME_WITHOUT_TIME_ZONE;
    static BasicLogicalType* TIMESTAMP_WITH_TIME_ZONE;
    static BasicLogicalType* TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    static BasicLogicalType* DATE;
    static BasicLogicalType* DOUBLE;
    static BasicLogicalType* INVALID_TYPE;
};
#endif  //FLINK_TNEL_LOGICALTYPE_H
