#ifndef OMNISTREAM_EXTERNALTYPEINFO_H
#define OMNISTREAM_EXTERNALTYPEINFO_H

#include <memory>

#include "TypeInformation.h"
#include "table/types/logical/LogicalType.h"

class ExternalTypeInfo : public TypeInformation {
public:
    ExternalTypeInfo(
        std::shared_ptr<LogicalType> dataType,
        TypeInformation* internalTypeInfo,
        bool isInternalInput,
        std::string conversionClass);

    ~ExternalTypeInfo() override;

    TypeSerializer* createTypeSerializer() override;

    BackendDataType getBackendId() const override;

    std::string name() override;

    LogicalType* getDataType() const;

    TypeInformation* getInternalTypeInfo() const;

    bool isInternalInput() const;

    const std::string& getConversionClass() const;

private:
    std::shared_ptr<LogicalType> dataType_;
    TypeInformation* internalTypeInfo_;
    bool isInternalInput_;
    std::string conversionClass_;
};

#endif // OMNISTREAM_EXTERNALTYPEINFO_H
