#include <gtest/gtest.h>
#include "runtime/state/InternalKeyContextImpl.h"

TEST(InternalKeyContextImplTest, SetCurrentKey)
{
    KeyGroupRange *range = new KeyGroupRange(0, 10);
    InternalKeyContextImpl<int> *context = new InternalKeyContextImpl<int>(range, 10);
    context->setCurrentKey(1);
    EXPECT_EQ(context->getCurrentKey(), 1);
    delete range;
    delete context;
}

TEST(InternalKeyContextImplTest, SetCurrentGroupIndex)
{
    KeyGroupRange *range = new KeyGroupRange(0, 10);
    InternalKeyContextImpl<int> *context = new InternalKeyContextImpl<int>(range, 10);
    context->setCurrentKeyGroupIndex(1);
    EXPECT_EQ(context->getCurrentKeyGroupIndex(), 1);
    delete range;
    delete context;
}

TEST(InternalKeyContextImplTest, GetKeyGroupRange)
{
    KeyGroupRange *range = new KeyGroupRange(0, 10);
    InternalKeyContextImpl<int> *context = new InternalKeyContextImpl<int>(range, 10);
    EXPECT_EQ(context->getKeyGroupRange()->equals(*range), true);
    delete range;
    delete context;
}

TEST(InternalKeyContextImplTest, GetNumberOfKeyGroups)
{
    KeyGroupRange *range = new KeyGroupRange(0, 10);
    InternalKeyContextImpl<int> *context = new InternalKeyContextImpl<int>(range, 10);
    EXPECT_EQ(context->getNumberOfKeyGroups(), 10);
    delete range;
    delete context;
}