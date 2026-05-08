#include <gtest/gtest.h>
#include "runtime/state/heap/HeapPriorityQueueSet.h"
#include "state/heap/HeapPriorityQueueElement.h"

namespace {
    class IntType : public HeapPriorityQueueElement {
    private:
        int val;

    public:
        IntType(int val) : val(val) {};
        int getKey() { return val; }
        size_t hashCode() { return std::hash<int>()(val); };

        struct SharedPtrHash {
            size_t operator()(const std::shared_ptr<IntType>& obj) const {
                if (obj) {
                    return obj->hashCode();
                } else {
                    return 0;
                }
            }
        };

        struct SharedPtrEqual {
            bool operator()(const std::shared_ptr<IntType>& lhs, const std::shared_ptr<IntType>& rhs) const {
                return lhs->getKey() == rhs->getKey();
            }
        };

        struct SharedPtrComparator {
            bool operator()(const std::shared_ptr<IntType>& lhs, const std::shared_ptr<IntType>& rhs) const {
                return lhs->getKey() < rhs->getKey();
            }
        };
    };
}


TEST(HeapPriorityQueueSetTest, Initialization)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);
    pq.add(std::make_shared<IntType>(1));
    EXPECT_EQ(pq.poll()->getKey(), 1);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueSetTest, Inserts)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);
    pq.add(std::make_shared<IntType>(1));
    pq.add(std::make_shared<IntType>(3));
    pq.add(std::make_shared<IntType>(2));
    pq.add(std::make_shared<IntType>(4));

    EXPECT_EQ(pq.poll()->getKey(), 1);
    EXPECT_EQ(pq.poll()->getKey(), 2);
    EXPECT_EQ(pq.poll()->getKey(), 3);
    EXPECT_EQ(pq.poll()->getKey(), 4);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueSetTest, HeadChange)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);
    EXPECT_EQ(pq.add(std::make_shared<IntType>(5)), true);
    EXPECT_EQ(pq.add(std::make_shared<IntType>(3)), true);
    EXPECT_EQ(pq.add(std::make_shared<IntType>(3)), false);
    EXPECT_EQ(pq.add(std::make_shared<IntType>(4)), false);
    EXPECT_EQ(pq.add(std::make_shared<IntType>(1)), true);
}

TEST(HeapPriorityQueueSetTest, ClearTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);
    EXPECT_EQ(pq.add(std::make_shared<IntType>(5)), true);
    EXPECT_EQ(pq.isEmpty(), false);
    EXPECT_EQ(pq.poll()->getKey(), 5);
    EXPECT_EQ(pq.poll(), nullptr);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueSetTest, DuplicateTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);
    pq.add(std::make_shared<IntType>(1));
    pq.add(std::make_shared<IntType>(1));

    EXPECT_EQ(pq.poll()->getKey(), 1);
    EXPECT_EQ(pq.poll(), nullptr);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueSetTest, RemoveTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);
    pq.add(std::make_shared<IntType>(1));
    pq.add(std::make_shared<IntType>(2));
    pq.add(std::make_shared<IntType>(3));
    pq.remove(std::make_shared<IntType>(2));
    EXPECT_EQ(pq.poll()->getKey(), 1);
    EXPECT_EQ(pq.poll()->getKey(), 3);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueSetTest, PeekTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);
    EXPECT_EQ(pq.peek(), nullptr);

    pq.add(std::make_shared<IntType>(5));
    pq.add(std::make_shared<IntType>(3));
    pq.add(std::make_shared<IntType>(1));

    EXPECT_EQ(pq.peek()->getKey(), 1);
    EXPECT_EQ(pq.peek()->getKey(), 1);
    EXPECT_EQ(pq.size(), 3);
}

TEST(HeapPriorityQueueSetTest, SizeTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);
    EXPECT_EQ(pq.size(), 0);

    pq.add(std::make_shared<IntType>(1));
    EXPECT_EQ(pq.size(), 1);

    pq.add(std::make_shared<IntType>(2));
    EXPECT_EQ(pq.size(), 2);

    pq.add(std::make_shared<IntType>(1));
    EXPECT_EQ(pq.size(), 2);

    pq.poll();
    EXPECT_EQ(pq.size(), 1);

    pq.remove(std::make_shared<IntType>(2));
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueSetTest, IteratorTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);
    pq.add(std::make_shared<IntType>(1));
    pq.add(std::make_shared<IntType>(2));
    pq.add(std::make_shared<IntType>(3));

    auto iter = pq.iterator();
    int count = 0;
    while (iter->hasNext()) {
        iter->next();
        count++;
    }
    EXPECT_EQ(count, 3);
}

TEST(HeapPriorityQueueSetTest, AddAllTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);
    std::vector<std::shared_ptr<IntType>> elements = {
        std::make_shared<IntType>(5),
        std::make_shared<IntType>(2),
        std::make_shared<IntType>(8),
        std::make_shared<IntType>(1)
    };

    pq.addAll(elements);
    EXPECT_EQ(pq.size(), 4);
    EXPECT_EQ(pq.poll()->getKey(), 1);
    EXPECT_EQ(pq.poll()->getKey(), 2);
    EXPECT_EQ(pq.poll()->getKey(), 5);
    EXPECT_EQ(pq.poll()->getKey(), 8);
}

TEST(HeapPriorityQueueSetTest, ToArrayTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);
    pq.add(std::make_shared<IntType>(3));
    pq.add(std::make_shared<IntType>(1));
    pq.add(std::make_shared<IntType>(2));

    auto arr = pq.toArray();
    EXPECT_EQ(arr.size(), 3);
}

TEST(HeapPriorityQueueSetTest, RemoveNonExistentTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);
    pq.add(std::make_shared<IntType>(1));
    pq.add(std::make_shared<IntType>(2));

    EXPECT_EQ(pq.remove(std::make_shared<IntType>(999)), false);
    EXPECT_EQ(pq.size(), 2);
}

TEST(HeapPriorityQueueSetTest, RemoveHeadTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);
    auto elem1 = std::make_shared<IntType>(1);
    auto elem2 = std::make_shared<IntType>(2);
    auto elem3 = std::make_shared<IntType>(3);

    pq.add(elem1);
    pq.add(elem2);
    pq.add(elem3);

    pq.remove(elem1);
    EXPECT_EQ(pq.size(), 2);
    EXPECT_EQ(pq.poll()->getKey(), 2);
    EXPECT_EQ(pq.poll()->getKey(), 3);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueSetTest, EmptyQueueOperationsTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);
    EXPECT_EQ(pq.isEmpty(), true);
    EXPECT_EQ(pq.size(), 0);
    EXPECT_EQ(pq.peek(), nullptr);
    EXPECT_EQ(pq.poll(), nullptr);
    EXPECT_EQ(pq.remove(std::make_shared<IntType>(1)), false);
}

TEST(HeapPriorityQueueSetTest, LargeScaleTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);

    for (int i = 500; i >= 1; --i) {
        pq.add(std::make_shared<IntType>(i));
    }
    EXPECT_EQ(pq.size(), 500);

    for (int i = 1; i <= 500; ++i) {
        EXPECT_EQ(pq.poll()->getKey(), i);
    }
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueSetTest, GetSubsetForKeyGroupBasic)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);

    pq.add(std::make_shared<IntType>(1));
    pq.add(std::make_shared<IntType>(2));
    pq.add(std::make_shared<IntType>(3));
    pq.add(std::make_shared<IntType>(4));
    pq.add(std::make_shared<IntType>(5));

    int totalSubsetSize = 0;
    for (int i = 0; i < keyGroupRange->getNumberOfKeyGroups(); ++i) {
        auto subset = pq.getSubsetForKeyGroup(i);
        ASSERT_NE(subset, nullptr);
        totalSubsetSize += static_cast<int>(subset->size());
    }
    EXPECT_EQ(totalSubsetSize, pq.size());
    EXPECT_EQ(totalSubsetSize, 5);
}

TEST(HeapPriorityQueueSetTest, GetSubsetForKeyGroupEmpty)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);

    for (int i = 0; i < keyGroupRange->getNumberOfKeyGroups(); ++i) {
        auto subset = pq.getSubsetForKeyGroup(i);
        ASSERT_NE(subset, nullptr);
        EXPECT_EQ(subset->size(), 0u);
    }
}

TEST(HeapPriorityQueueSetTest, GetSubsetForKeyGroupAfterRemove)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);

    pq.add(std::make_shared<IntType>(1));
    pq.add(std::make_shared<IntType>(2));
    pq.add(std::make_shared<IntType>(3));

    pq.remove(std::make_shared<IntType>(2));

    int totalSubsetSize = 0;
    for (int i = 0; i < keyGroupRange->getNumberOfKeyGroups(); ++i) {
        auto subset = pq.getSubsetForKeyGroup(i);
        ASSERT_NE(subset, nullptr);
        totalSubsetSize += static_cast<int>(subset->size());
    }
    EXPECT_EQ(totalSubsetSize, 2);
}

TEST(HeapPriorityQueueSetTest, GetSubsetForKeyGroupAfterPoll)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);

    pq.add(std::make_shared<IntType>(1));
    pq.add(std::make_shared<IntType>(2));
    pq.add(std::make_shared<IntType>(3));

    pq.poll();

    int totalSubsetSize = 0;
    for (int i = 0; i < keyGroupRange->getNumberOfKeyGroups(); ++i) {
        auto subset = pq.getSubsetForKeyGroup(i);
        ASSERT_NE(subset, nullptr);
        totalSubsetSize += static_cast<int>(subset->size());
    }
    EXPECT_EQ(totalSubsetSize, 2);
}

TEST(HeapPriorityQueueSetTest, GetSubsetForKeyGroupConsistency)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);

    for (int i = 1; i <= 10; ++i) {
        pq.add(std::make_shared<IntType>(i));
    }

    std::unordered_set<int> allKeysInSubsets;
    int totalSubsetSize = 0;
    for (int i = 0; i < keyGroupRange->getNumberOfKeyGroups(); ++i) {
        auto subset = pq.getSubsetForKeyGroup(i);
        ASSERT_NE(subset, nullptr);
        for (const auto& elem : *subset) {
            EXPECT_EQ(allKeysInSubsets.count(elem->getKey()), 0u)
                << "Key " << elem->getKey() << " found in multiple subsets";
            allKeysInSubsets.insert(elem->getKey());
        }
        totalSubsetSize += static_cast<int>(subset->size());
    }
    EXPECT_EQ(totalSubsetSize, 10);
    EXPECT_EQ(allKeysInSubsets.size(), 10u);
}

TEST(HeapPriorityQueueSetTest, MixedOperationsTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<IntType>, IntType::SharedPtrComparator>(
            keyGroupRange.get(),
            keyGroupRange->getNumberOfKeyGroups(),
            6);

    pq.add(std::make_shared<IntType>(5));
    pq.add(std::make_shared<IntType>(3));
    EXPECT_EQ(pq.poll()->getKey(), 3);

    pq.add(std::make_shared<IntType>(1));
    pq.add(std::make_shared<IntType>(4));
    EXPECT_EQ(pq.peek()->getKey(), 1);

    pq.remove(std::make_shared<IntType>(4));
    EXPECT_EQ(pq.poll()->getKey(), 1);
    EXPECT_EQ(pq.poll()->getKey(), 5);
    EXPECT_EQ(pq.isEmpty(), true);
}