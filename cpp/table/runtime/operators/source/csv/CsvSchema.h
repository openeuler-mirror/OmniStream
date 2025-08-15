/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef CSVSCHEMA_H
#define CSVSCHEMA_H

#pragma once
#include <string>
#include <vector>

#include "type/data_type.h"

namespace omnistream {
namespace csv {

class CsvSchema {
public:
    explicit CsvSchema(const std::vector<omniruntime::type::DataTypeId>& types) : types_(types)
    {
        arity_                 = static_cast<int>(types_.size());
        columnSeparator_       = ',';
        quoteChar_             = '"';
        escapeChar_            = '\\';
        allowComments_         = false;
        arrayElementSeparator_ = ",";
        nullValue_             = "";
    }

    // Setter methods
    void setColumnSeparator(char separator)
    {
        columnSeparator_ = separator;
    }

    void setQuoteChar(char quote)
    {
        quoteChar_ = quote;
    }

    void setAllowComments(bool allow)
    {
        allowComments_ = allow;
    }

    void setArrayElementSeparator(const std::string& separator)
    {
        arrayElementSeparator_ = separator;
    }

    void setEscapeChar(char escape)
    {
        escapeChar_ = escape;
    }

    void setNullValue(const std::string& nullValue)
    {
        nullValue_ = nullValue;
    }

    // Getter methods
    char getColumnSeparator() const
    {
        return columnSeparator_;
    }

    char getQuoteChar() const
    {
        return quoteChar_;
    }

    char getEscapeChar() const
    {
        return escapeChar_;
    }

    bool getAllowComments() const
    {
        return allowComments_;
    }

    std::string getArrayElementSeparator() const
    {
        return arrayElementSeparator_;
    }

    int getArity() const
    {
        return arity_;
    }

    std::vector<omniruntime::type::DataTypeId> getTypes() const
    {
        return types_;
    }

    omniruntime::type::DataTypeId getTypeAtIdx(int idx) const
    {
        return types_[idx];
    }

private:
    char columnSeparator_;
    char quoteChar_;
    char escapeChar_;
    bool allowComments_;
    std::string arrayElementSeparator_;
    std::string nullValue_;

    int arity_;
    std::vector<omniruntime::type::DataTypeId> types_;
};

}  // namespace csv
}  // namespace omnistream
#endif
