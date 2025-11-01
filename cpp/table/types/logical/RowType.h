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

#ifndef FLINK_TNEL_ROWTYPE_H
#define FLINK_TNEL_ROWTYPE_H

#include <string>
#include <vector>
#include "LogicalType.h"

using string = std::string;
namespace omnistream {
    class RowField {
        public:
            RowField(string name, LogicalType *type, string description);

            RowField(const string &name, LogicalType *type);

            [[nodiscard]] LogicalType *getType() const;

        private:
            string name_;
            LogicalType *type_;
            string description_;
    };

    class RowType : public LogicalType {
        public:
            RowType(bool isNull, const std::vector<RowField> &fields);
            RowType(bool isNull, const std::vector<std::string> &typeName);
            std::vector<LogicalType *> getChildren() override;

        private:
            std::vector<RowField> fields_;
            std::vector<LogicalType *> types;
        };
}

#endif // FLINK_TNEL_ROWTYPE_H
