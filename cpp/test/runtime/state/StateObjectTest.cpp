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

#include <gtest/gtest.h>
#include "runtime/state/StateObject.h"

// Concrete implementation for testing
class TestStateObject : public StateObject {
private:
    long size;
    bool discarded = false;

public:
    explicit TestStateObject(size_t sz) : size(sz) {}

    void DiscardState() override {
        discarded = true;
    }

    long GetStateSize() const override {
        return size;
    }

    bool IsDiscarded() const {
        return discarded;
    }

    std::string ToString() const override
    {
        return "";
    }
};

// Unit tests
TEST(StateObjectTest, GetStateSizeReturnsCorrectValue) {
    TestStateObject obj(100);
    EXPECT_EQ(obj.GetStateSize(), 100);
}

TEST(StateObjectTest, DiscardStateMarksObjectAsDiscarded) {
    TestStateObject obj(0);
    EXPECT_FALSE(obj.IsDiscarded());
    obj.DiscardState();
    EXPECT_TRUE(obj.IsDiscarded());
}

