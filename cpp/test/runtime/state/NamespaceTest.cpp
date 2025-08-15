//
// Created by xichen on 10/29/24.
//
#include <gtest/gtest.h>
#include "runtime/state/VoidNamespace.h"

TEST(NamespaceTest, VoidNamespace)
{
    VoidNamespace vn1;
    VoidNamespace vn2;

    std::hash<VoidNamespace> myHash;
    std::size_t hashValue = myHash(vn1);
    EXPECT_EQ(hashValue, 99);

    std::equal_to<VoidNamespace> myEqual;
    bool isEqual = myEqual(vn1, vn2);

    EXPECT_EQ(isEqual, true);
}

