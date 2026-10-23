/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "StreamingSemiAntiJoinOperator.h"

#include "core/utils/key_type_traits.h"
#include "state/JoinRecordStateViews.h"
#include "state/OuterJoinRecordStateViews.h"
#include "table/data/util/RowDataUtil.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "table/typeutils/InternalTypeInfo.h"

namespace omnistream {
template <typename K>
void StreamingSemiAntiJoinOperator<K>::processBatch(omnistream::VectorBatch* input, bool isLeft)
{
    outputRows_.clear();
    outputKinds_.clear();
    outputTimestamps_.clear();
    auto inputGuard = std::unique_ptr<omnistream::VectorBatch>(input);
    for (int32_t i = 0; i < input->GetRowCount(); ++i) {
        currentInputRowTimestamp_ = input->getTimestamp(i);
        auto row = std::shared_ptr<RowData>(input->extractRowData(i));
        if (row == nullptr) {
            THROW_RUNTIME_ERROR("Failed to extract input RowData for streaming semi/anti join");
        }
        if (isLeft) {
            processElementLeft(row);
        } else {
            processElementRight(row);
        }
    }

    auto* outputVectorBatch = buildOutputVectorBatch();
    if (outputVectorBatch != nullptr) {
        this->collector->collect(outputVectorBatch);
    }
}

template <typename K>
void StreamingSemiAntiJoinOperator<K>::processElementLeft(const std::shared_ptr<RowData>& input)
{
    auto& keySelector = this->keySelectorLeft_;
    auto key = keySelector->getKey(input.get());
    this->setCurrentKey(key);

    auto associatedRecords = AbstractStreamingJoinOperator<K>::of(input, true, rightRecordStateView_.get());
    if (associatedRecords.empty()) {
        if (isAntiJoin_) {
            collectLeft(input, input->getRowKind());
        }
    } else if (!isAntiJoin_) {
        collectLeft(input, input->getRowKind());
    }

    bool isAccumulateMsg = RowDataUtil::isAccumulateMsg(input->getRowKind());
    input->setRowKind(RowKind::INSERT);
    if (isAccumulateMsg) {
        leftRecordStateView_->addRecord(input, static_cast<int32_t>(associatedRecords.size()));
    } else {
        leftRecordStateView_->retractRecord(input);
    }

    if constexpr (KeyTypeTraits<K>::isRowKey) {
        delete key;
    }
}

template <typename K>
void StreamingSemiAntiJoinOperator<K>::processElementRight(const std::shared_ptr<RowData>& input)
{
    bool isAccumulateMsg = RowDataUtil::isAccumulateMsg(input->getRowKind());
    auto inputRowKind = input->getRowKind();
    input->setRowKind(RowKind::INSERT);

    auto& keySelector = this->keySelectorRight_;
    auto key = keySelector->getKey(input.get());
    this->setCurrentKey(key);

    auto associatedRecords = AbstractStreamingJoinOperator<K>::of(input, false, leftRecordStateView_.get());
    if (isAccumulateMsg) {
        rightRecordStateView_->addRecord(input);
        for (const auto& [other, numOfAssociations] : associatedRecords) {
            if (numOfAssociations == 0) {
                collectLeft(other, isAntiJoin_ ? RowKind::DELETE : inputRowKind);
            }
            leftRecordStateView_->updateNumOfAssociations(other, numOfAssociations + 1);
        }
    } else {
        rightRecordStateView_->retractRecord(input);
        for (const auto& [other, numOfAssociations] : associatedRecords) {
            if (numOfAssociations == 1) {
                collectLeft(other, isAntiJoin_ ? RowKind::INSERT : inputRowKind);
            }
            leftRecordStateView_->updateNumOfAssociations(other, numOfAssociations - 1);
        }
    }

    if constexpr (KeyTypeTraits<K>::isRowKey) {
        delete key;
    }
}

template <typename K>
void StreamingSemiAntiJoinOperator<K>::open()
{
    AbstractStreamingJoinOperator<K>::open();
    auto* leftRecordType = InternalTypeInfo::ofRowType(
        new omnistream::RowType(true, this->description["leftInputTypes"].template get<std::vector<std::string>>()));
    auto* rightRecordType = InternalTypeInfo::ofRowType(
        new omnistream::RowType(true, this->description["rightInputTypes"].template get<std::vector<std::string>>()));
    leftRecordStateView_ =
        OuterJoinRecordStateViews::create<K>(this->getRuntimeContext(), "left-records", leftRecordType);
    rightRecordStateView_ = JoinRecordStateViews::create(this->getRuntimeContext(), "right-records", rightRecordType);

    std::vector<int> leftKeyTypes;
    std::vector<int> rightKeyTypes;
    for (auto kIndex : this->leftKeyIndex) {
        leftKeyTypes.push_back(this->leftInputTypes[kIndex]);
    }
    for (auto kIndex : this->rightKeyIndex) {
        rightKeyTypes.push_back(this->rightInputTypes[kIndex]);
    }
    if (leftKeyTypes != rightKeyTypes) {
        throw std::runtime_error("Left key types do not match right key types");
    }

    this->keySelectorLeft_ = std::make_unique<KeySelector<K>>(leftKeyTypes, this->leftKeyIndex);
    this->keySelectorRight_ = std::make_unique<KeySelector<K>>(rightKeyTypes, this->rightKeyIndex);
}

template <typename K>
omnistream::VectorBatch* StreamingSemiAntiJoinOperator<K>::buildOutputVectorBatch()
{
    if (outputRows_.empty()) {
        return nullptr;
    }
    const auto outputRowCount = static_cast<int32_t>(outputRows_.size());
    auto* res = new omnistream::VectorBatch(outputRowCount);
    auto isNull = [](RowData* row, int32_t column) { return row == nullptr || row->isNullAt(column); };
    auto appendColumn = [&](auto* vector, int32_t inputColumn, auto valueGetter) {
        for (int32_t outputRow = 0; outputRow < outputRowCount; ++outputRow) {
            RowData* row = outputRows_[outputRow].get();
            if (isNull(row, inputColumn)) {
                vector->SetNull(outputRow);
            } else {
                auto value = valueGetter(row, inputColumn);
                vector->SetValue(outputRow, value);
            }
        }
        res->Append(vector);
    };
    auto appendDecimal128Column = [&](int32_t inputColumn) {
        auto* vector = new omniruntime::vec::Vector<omniruntime::type::Decimal128>(
            outputRowCount, omniruntime::type::DataTypeId::OMNI_DECIMAL128);
        for (int32_t outputRow = 0; outputRow < outputRowCount; ++outputRow) {
            RowData* row = outputRows_[outputRow].get();
            if (isNull(row, inputColumn)) {
                vector->SetNull(outputRow);
                continue;
            }
            auto* binaryRow = dynamic_cast<BinaryRowData*>(row);
            if (binaryRow == nullptr) {
                delete vector;
                throw std::runtime_error("DECIMAL128 semi/anti join output requires BinaryRowData");
            }
            std::unique_ptr<omniruntime::type::Decimal128> value(binaryRow->getDecimal128(inputColumn, 0));
            if (value == nullptr) {
                delete vector;
                throw std::runtime_error("Failed to read DECIMAL128 value from semi/anti join row");
            }
            vector->SetValue(outputRow, *value);
        }
        res->Append(vector);
    };

    for (int32_t inputColumn = 0; inputColumn < static_cast<int32_t>(this->leftInputTypes.size()); ++inputColumn) {
        switch (this->leftInputTypes[inputColumn]) {
            case omniruntime::type::DataTypeId::OMNI_BYTE:
                appendColumn(
                    new omniruntime::vec::Vector<int8_t>(
                        outputRowCount, static_cast<omniruntime::type::DataTypeId>(this->leftInputTypes[inputColumn])),
                    inputColumn,
                    [](RowData* row, int32_t column) { return *reinterpret_cast<int8_t*>(row->getLong(column)); });
                break;
            case omniruntime::type::DataTypeId::OMNI_SHORT:
                appendColumn(
                    new omniruntime::vec::Vector<int16_t>(
                        outputRowCount, static_cast<omniruntime::type::DataTypeId>(this->leftInputTypes[inputColumn])),
                    inputColumn,
                    [](RowData* row, int32_t column) { return *reinterpret_cast<int16_t*>(row->getLong(column)); });
                break;
            case omniruntime::type::DataTypeId::OMNI_INT:
            case omniruntime::type::DataTypeId::OMNI_DATE32:
            case omniruntime::type::DataTypeId::OMNI_TIME32:
            case omniruntime::type::DataTypeId::OMNI_INTERVAL_MONTHS:
                appendColumn(
                    new omniruntime::vec::Vector<int32_t>(
                        outputRowCount, static_cast<omniruntime::type::DataTypeId>(this->leftInputTypes[inputColumn])),
                    inputColumn,
                    [](RowData* row, int32_t column) { return *row->getInt(column); });
                break;
            case omniruntime::type::DataTypeId::OMNI_LONG:
            case omniruntime::type::DataTypeId::OMNI_DATE64:
            case omniruntime::type::DataTypeId::OMNI_TIME64:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME:
            case omniruntime::type::DataTypeId::OMNI_DECIMAL64:
                appendColumn(
                    new omniruntime::vec::Vector<int64_t>(
                        outputRowCount, static_cast<omniruntime::type::DataTypeId>(this->leftInputTypes[inputColumn])),
                    inputColumn,
                    [](RowData* row, int32_t column) { return *reinterpret_cast<int64_t*>(row->getLong(column)); });
                break;
            case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                appendColumn(
                    new omniruntime::vec::Vector<double>(
                        outputRowCount, static_cast<omniruntime::type::DataTypeId>(this->leftInputTypes[inputColumn])),
                    inputColumn,
                    [](RowData* row, int32_t column) { return *reinterpret_cast<double*>(row->getLong(column)); });
                break;
            case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                appendColumn(
                    new omniruntime::vec::Vector<bool>(
                        outputRowCount, static_cast<omniruntime::type::DataTypeId>(this->leftInputTypes[inputColumn])),
                    inputColumn,
                    [](RowData* row, int32_t column) { return *row->getBool(column); });
                break;
            case omniruntime::type::DataTypeId::OMNI_DECIMAL128: appendDecimal128Column(inputColumn); break;
            case omniruntime::type::DataTypeId::OMNI_CHAR:
            case omniruntime::type::DataTypeId::OMNI_VARCHAR:
            case omniruntime::type::DataTypeId::OMNI_VARBINARY:
                appendColumn(
                    new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(
                        outputRowCount),
                    inputColumn,
                    [](RowData* row, int32_t column) { return row->getStringView(column); });
                break;
            default:
                delete res;
                throw std::runtime_error(
                    "Unsupported semi/anti join output column type: " +
                    std::to_string(this->leftInputTypes[inputColumn]));
        }
    }
    for (int32_t index = 0; index < outputRowCount; ++index) {
        res->setRowKind(index, outputKinds_[index]);
        res->setTimestamp(index, outputTimestamps_[index]);
    }
    return res;
}

template class StreamingSemiAntiJoinOperator<RowData*>;
template class StreamingSemiAntiJoinOperator<long>;
} // namespace omnistream
