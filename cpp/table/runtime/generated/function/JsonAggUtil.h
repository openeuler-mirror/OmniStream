/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * Description: Shared helpers for JSON_OBJECTAGG / JSON_ARRAYAGG native aggregate handlers.
 * Serializes a scalar value (read from a RowData or a VectorBatch column) into its JSON text
 * representation, matching Flink JSON constructor value rendering:
 *   VARCHAR  -> "escaped string"
 *   INT/LONG -> number literal
 *   BOOLEAN  -> true | false
 *   DOUBLE   -> number literal (see caveat below)
 *   SQL NULL -> null
 *
 * NOTE (defensive assumptions, local-only build):
 *  - DOUBLE textual formatting may not be byte-identical to Flink's Jackson output; VARCHAR/INT/
 *    LONG/BOOLEAN are the primary supported value types (see design doc §4.1).
 *  - Value NULL handling (ON NULL) is decided by the caller BEFORE calling AppendJsonValue*.
 */

#ifndef FLINK_TNEL_JSON_AGG_UTIL_H
#define FLINK_TNEL_JSON_AGG_UTIL_H

#include <string>
#include <string_view>
#include <sstream>
#include <stdexcept>
#include <cstdio>
#include "table/data/RowData.h"
// Brings in omniruntime::vec (Vector, BaseVector, LargeStringContainer, DictionaryContainer,
// OMNI_FLAT, vector_helper) and omniruntime::type (DataTypeId) transitively.
#include "table/data/vectorbatch/VectorBatch.h"

namespace omnistream {
namespace jsonagg {

using omniruntime::type::DataTypeId;

// Append `s` as a JSON string literal (surrounding quotes + escaping) to `out`.
inline void AppendJsonEscapedString(std::string& out, std::string_view s)
{
    out.push_back('"');
    for (char c : s) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
                    out += buf;
                } else {
                    out.push_back(c);
                }
        }
    }
    out.push_back('"');
}

inline void AppendJsonDouble(std::string& out, double v)
{
    std::ostringstream oss;
    oss.precision(17);
    oss << v;
    out += oss.str();
}

// Read a VARCHAR value from a batch column at `row`, handling FLAT and DICTIONARY encodings.
inline std::string_view ReadStringFromColumn(omniruntime::vec::BaseVector* col, int row)
{
    if (col->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
        auto casted =
            reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(col);
        return casted->GetValue(row);
    }
    auto casted = reinterpret_cast<omniruntime::vec::Vector<
        omniruntime::vec::DictionaryContainer<std::string_view, omniruntime::vec::LargeStringContainer>>*>(col);
    return casted->GetValue(row);
}

// Serialize batch column value at `row` (already known non-null by caller) as JSON text into `out`.
inline void AppendJsonValueFromColumn(
    std::string& out, omniruntime::vec::BaseVector* col, int row, DataTypeId typeId)
{
    switch (typeId) {
        case DataTypeId::OMNI_VARCHAR: {
            AppendJsonEscapedString(out, ReadStringFromColumn(col, row));
            break;
        }
        case DataTypeId::OMNI_INT: {
            out += std::to_string(reinterpret_cast<omniruntime::vec::Vector<int>*>(col)->GetValue(row));
            break;
        }
        case DataTypeId::OMNI_LONG: {
            out += std::to_string(reinterpret_cast<omniruntime::vec::Vector<long>*>(col)->GetValue(row));
            break;
        }
        case DataTypeId::OMNI_BOOLEAN: {
            out += reinterpret_cast<omniruntime::vec::Vector<bool>*>(col)->GetValue(row) ? "true" : "false";
            break;
        }
        case DataTypeId::OMNI_DOUBLE: {
            AppendJsonDouble(out, reinterpret_cast<omniruntime::vec::Vector<double>*>(col)->GetValue(row));
            break;
        }
        default:
            throw std::runtime_error("JSON aggregate: unsupported value type for column serialization.");
    }
}

// Serialize RowData field at `idx` (already known non-null by caller) as JSON text into `out`.
inline void AppendJsonValueFromRow(std::string& out, RowData* row, int idx, DataTypeId typeId)
{
    switch (typeId) {
        case DataTypeId::OMNI_VARCHAR: {
            AppendJsonEscapedString(out, row->getStringView(idx));
            break;
        }
        case DataTypeId::OMNI_INT: {
            out += std::to_string(*row->getInt(idx));
            break;
        }
        case DataTypeId::OMNI_LONG: {
            out += std::to_string(*row->getLong(idx));
            break;
        }
        case DataTypeId::OMNI_BOOLEAN: {
            out += (*row->getBool(idx)) ? "true" : "false";
            break;
        }
        case DataTypeId::OMNI_DOUBLE: {
            AppendJsonDouble(out, *row->getDouble(idx));
            break;
        }
        default:
            throw std::runtime_error("JSON aggregate: unsupported value type for row serialization.");
    }
}

} // namespace jsonagg
} // namespace omnistream

#endif // FLINK_TNEL_JSON_AGG_UTIL_H
