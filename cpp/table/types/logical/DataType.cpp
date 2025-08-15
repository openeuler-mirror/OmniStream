#include "table/types/logical/DataType.h"

namespace omnistream
{
    // Since most methods are inline or pure virtual,
    // we only need this empty implementation file as a placeholder
    // for potential future non-inline implementations

    DataType::DataType(std::shared_ptr<LogicalType> logicalType, std::shared_ptr<void> conversionClass)
        : logicalType_(logicalType),
          conversionClass_(ensureConversionClass(logicalType, conversionClass)) {
        performEarlyClassValidation(logicalType, conversionClass_);
    }

    std::shared_ptr<LogicalType> DataType::getLogicalType() const {
        return logicalType_;
    }

    std::shared_ptr<void> DataType::getConversionClass() const {
        return conversionClass_;
    }
}