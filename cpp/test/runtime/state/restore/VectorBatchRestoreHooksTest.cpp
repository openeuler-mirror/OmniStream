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

#include <gtest/gtest.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include "runtime/state/restore/vb/VectorBatchRestoreHooks.h"

using namespace omnistream;

namespace {

TEST(VectorBatchRestoreHooksTest, TransformKVDataThrowsWhenNotOverridden)
{
    VectorBatchRestoreHooks hooks;

    try {
        hooks.transformKVData(std::vector<int8_t>{1}, std::vector<int8_t>{2}, 3, nullptr);
        FAIL() << "Expected std::runtime_error";
    } catch (const std::runtime_error& error) {
        EXPECT_EQ(std::string(error.what()), "transformKVData not implemented by this adaptor");
    }
}

TEST(VectorBatchRestoreHooksTest, RetrieveKVRowDataThrowsWhenNotOverridden)
{
    VectorBatchRestoreHooks hooks;

    try {
        hooks.retrieveKVRowData(std::vector<int8_t>{1}, std::vector<int8_t>{2}, 3, nullptr);
        FAIL() << "Expected std::runtime_error";
    } catch (const std::runtime_error& error) {
        EXPECT_EQ(std::string(error.what()), "retrieveKVRowData not implemented by this adaptor");
    }
}

} // namespace
