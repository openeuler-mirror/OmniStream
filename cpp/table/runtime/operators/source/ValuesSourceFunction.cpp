/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "ValuesSourceFunction.h"

#include <stdexcept>
#include <string>
#include <string_view>

#include "OmniOperatorJIT/core/src/type/TimestampConversion.h"
#include "OmniOperatorJIT/core/src/type/decimal128.h"
#include "table/data/TimestampData.h"

using namespace omniruntime::type;

namespace omnistream {

namespace {

// Aligns with Flink DecimalData.fromBigDecimal: significant digits of the unscaled
// integer must be <= precision (0 has precision 1). Throws out_of_range on overflow.
void EnsureDecimalPrecision(const std::string& unscaledAbs, int32_t precision)
{
    if (unscaledAbs.empty()) {
        throw std::invalid_argument("empty unscaled decimal");
    }
    if (precision <= 0) {
        return;
    }
    size_t start = 0;
    while (start + 1 < unscaledAbs.size() && unscaledAbs[start] == '0') {
        ++start;
    }
    if (static_cast<int32_t>(unscaledAbs.size() - start) > precision) {
        throw std::out_of_range("decimal precision overflow");
    }
}

std::string BuildUnscaledAbs(const std::string& value, int32_t scale, bool& negative)
{
    std::string valueStr = value;
    negative = false;
    if (!valueStr.empty() && valueStr[0] == '-') {
        negative = true;
        valueStr = valueStr.substr(1);
    }
    std::string intPart;
    std::string fracPart;
    size_t dotPos = valueStr.find('.');
    if (dotPos != std::string::npos) {
        intPart = valueStr.substr(0, dotPos);
        fracPart = valueStr.substr(dotPos + 1);
    } else {
        intPart = valueStr;
        fracPart = "";
    }
    if (static_cast<int32_t>(fracPart.length()) < scale) {
        fracPart += std::string(scale - fracPart.length(), '0');
    } else if (static_cast<int32_t>(fracPart.length()) > scale) {
        fracPart = fracPart.substr(0, scale);
    }
    std::string unscaledAbs = intPart + fracPart;
    if (unscaledAbs.empty()) {
        throw std::invalid_argument("invalid decimal literal: " + value);
    }
    return unscaledAbs;
}

long ParseDecimal64ToUnscaledLong(const std::string& value, int32_t precision, int32_t scale)
{
    bool negative = false;
    std::string unscaledAbs = BuildUnscaledAbs(value, scale, negative);
    EnsureDecimalPrecision(unscaledAbs, precision);
    long unscaledValue = std::stol(unscaledAbs);
    if (negative) {
        unscaledValue = -unscaledValue;
    }
    return unscaledValue;
}

Decimal128 ParseDecimal128ToValue(const std::string& value, int32_t precision, int32_t scale)
{
    bool negative = false;
    std::string unscaledAbs = BuildUnscaledAbs(value, scale, negative);
    EnsureDecimalPrecision(unscaledAbs, precision);
    if (negative) {
        unscaledAbs = "-" + unscaledAbs;
    }
    return Decimal128(std::string_view(unscaledAbs));
}

std::string CellToString(const nlohmann::json& cell)
{
    if (cell.is_string()) {
        return cell.get<std::string>();
    }
    if (cell.is_boolean()) {
        return cell.get<bool>() ? "true" : "false";
    }
    if (cell.is_number()) {
        return cell.dump();
    }
    throw std::invalid_argument("unsupported VALUES literal json cell type");
}

} // namespace

ValuesSourceFunction::ValuesSourceFunction(const nlohmann::json& config)
    : tuples_(config.contains("tuples") ? config["tuples"] : nlohmann::json::array()),
      isRunning_(true),
      emitted_(false)
{
    if (!config.contains("outputTypes") || !config["outputTypes"].is_array()) {
        throw std::invalid_argument("VALUES source description missing outputTypes");
    }
    for (const auto& typeJson : config["outputTypes"]) {
        std::string typeStr = typeJson.get<std::string>();
        DataTypeId typeId = LogicalType::flinkTypeToOmniTypeId(typeStr);
        if (typeId == DataTypeId::OMNI_INVALID) {
            throw std::runtime_error("unsupported VALUES output type: " + typeStr);
        }
        fieldTypes_.push_back(typeId);
        auto [precision, scale] = LogicalType::parseDecimalPrecisionScale(typeStr);
        precisions_.push_back(precision);
        scales_.push_back(scale);
    }
}

void ValuesSourceFunction::run(SourceContext* ctx)
{
    int taskId = this->getRuntimeContext()->getIndexOfThisSubtask();
    if (taskId > 0) {
        return;
    }
    while (isRunning_.load() && !reachedEnd()) {
        omnistream::VectorBatch* batch = nextRecord();
        if (batch == nullptr) {
            break;
        }
        ctx->collect(batch);
    }
}

void ValuesSourceFunction::cancel()
{
    isRunning_.store(false);
}

void ValuesSourceFunction::close()
{
    AbstractRichFunction::close();
}

bool ValuesSourceFunction::reachedEnd() const
{
    return emitted_ || tuples_.empty();
}

omnistream::VectorBatch* ValuesSourceFunction::nextRecord()
{
    if (reachedEnd()) {
        return nullptr;
    }
    emitted_ = true;
    return convert(tuples_, fieldTypes_, precisions_, scales_);
}

omnistream::VectorBatch* ValuesSourceFunction::convert(
    const nlohmann::json& tuples,
    const std::vector<DataTypeId>& fieldTypes,
    const std::vector<int32_t>& precisions,
    const std::vector<int32_t>& scales)
{
    if (tuples.empty()) {
        return new omnistream::VectorBatch(0);
    }
    size_t rowCount = tuples.size();
    size_t colCount = fieldTypes.size();
    auto* vectorBatch = omnistream::VectorBatch::CreateVectorBatch(static_cast<int>(rowCount), fieldTypes);
    for (size_t i = 0; i < colCount; ++i) {
        if (fieldTypes[i] == DataTypeId::OMNI_DECIMAL64) {
            int32_t precision = precisions[i] > 0 ? precisions[i] : DECIMAL64_DEFAULT_PRECISION;
            int32_t scale = scales[i];
            vectorBatch->Get(static_cast<int>(i))
                ->SetDataType(std::make_shared<Decimal64DataType>(precision, scale));
        } else if (fieldTypes[i] == DataTypeId::OMNI_DECIMAL128) {
            int32_t precision = precisions[i] > 0 ? precisions[i] : DECIMAL128_DEFAULT_PRECISION;
            int32_t scale = scales[i];
            vectorBatch->Get(static_cast<int>(i))
                ->SetDataType(std::make_shared<Decimal128DataType>(precision, scale));
        }
    }

    for (size_t rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        const nlohmann::json& row = tuples[rowIndex];
        if (!row.is_array() || row.size() != colCount) {
            throw std::runtime_error("VALUES tuple row width mismatch");
        }
        for (size_t colIndex = 0; colIndex < colCount; ++colIndex) {
            const nlohmann::json& cell = row[colIndex];
            if (cell.is_null()) {
                vectorBatch->Get(static_cast<int>(colIndex))->SetNull(static_cast<int32_t>(rowIndex));
                continue;
            }
            std::string value = CellToString(cell);
            DataTypeId type = fieldTypes[colIndex];
            switch (type) {
                case DataTypeId::OMNI_BOOLEAN: {
                    vectorBatch->SetValueAt(
                        static_cast<int>(colIndex), static_cast<int>(rowIndex), value == "true");
                    break;
                }
                case DataTypeId::OMNI_INT: {
                    vectorBatch->SetValueAt(static_cast<int>(colIndex), static_cast<int>(rowIndex), std::stoi(value));
                    break;
                }
                case DataTypeId::OMNI_DATE32: {
                    auto result =
                        omniruntime::type::util::fromDateString(std::string_view(value.data(), value.size()));
                    if (result.hasError()) {
                        throw std::invalid_argument("invalid date literal: " + value);
                    }
                    vectorBatch->SetValueAt(static_cast<int>(colIndex), static_cast<int>(rowIndex), result.value());
                    break;
                }
                case DataTypeId::OMNI_LONG: {
                    vectorBatch->SetValueAt(static_cast<int>(colIndex), static_cast<int>(rowIndex), std::stoll(value));
                    break;
                }
                case DataTypeId::OMNI_DOUBLE: {
                    vectorBatch->SetValueAt(static_cast<int>(colIndex), static_cast<int>(rowIndex), std::stod(value));
                    break;
                }
                case DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE: {
                    vectorBatch->SetValueAt(
                        static_cast<int>(colIndex), static_cast<int>(rowIndex), TimestampData::stringToMillisOfDay(value));
                    break;
                }
                case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                case DataTypeId::OMNI_TIMESTAMP: {
                    vectorBatch->SetValueAt(
                        static_cast<int>(colIndex), static_cast<int>(rowIndex), TimestampData::stringToEpochMillis(value));
                    break;
                }
                case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                    vectorBatch->SetValueAt(
                        static_cast<int>(colIndex),
                        static_cast<int>(rowIndex),
                        TimestampData::fromLocalTimeString(value).getMillisecond());
                    break;
                }
                case DataTypeId::OMNI_CHAR:
                case DataTypeId::OMNI_VARCHAR: {
                    auto stringVec = reinterpret_cast<
                        omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                        vectorBatch->Get(static_cast<int>(colIndex)));
                    std::string_view strView(value.data(), value.size());
                    stringVec->SetValue(static_cast<int>(rowIndex), strView);
                    break;
                }
                case DataTypeId::OMNI_DECIMAL64: {
                    int32_t precision = precisions[colIndex] > 0 ? precisions[colIndex] : DECIMAL64_DEFAULT_PRECISION;
                    vectorBatch->SetValueAt(
                        static_cast<int>(colIndex),
                        static_cast<int>(rowIndex),
                        ParseDecimal64ToUnscaledLong(value, precision, scales[colIndex]));
                    break;
                }
                case DataTypeId::OMNI_DECIMAL128: {
                    int32_t precision = precisions[colIndex] > 0 ? precisions[colIndex] : DECIMAL128_DEFAULT_PRECISION;
                    Decimal128 decimalValue = ParseDecimal128ToValue(value, precision, scales[colIndex]);
                    vectorBatch->SetValueAt(static_cast<int>(colIndex), static_cast<int>(rowIndex), decimalValue);
                    break;
                }
                default:
                    throw std::runtime_error("VALUES: unsupported native type " + std::to_string(type));
            }
        }
    }
    return vectorBatch;
}

} // namespace omnistream
