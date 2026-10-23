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

#pragma once

#include "JoinRecordStateView.h"

#include <cstdint>
#include <tuple>

namespace omnistream {
class OuterJoinRecordStateView : public JoinRecordStateView {
public:
    ~OuterJoinRecordStateView() override = default;

    using JoinRecordStateView::addRecord;
    virtual void addRecord(const std::shared_ptr<RowData>& record, int32_t numOfAssociations) = 0;

    virtual void updateNumOfAssociations(const std::shared_ptr<RowData>& record, int32_t numOfAssociations) = 0;

    virtual std::unique_ptr<JoinRecordStateView::RecordsAndNumOfAssociationsIterator>
    getRecordsAndNumOfAssociations() = 0;
};
} // namespace omnistream
