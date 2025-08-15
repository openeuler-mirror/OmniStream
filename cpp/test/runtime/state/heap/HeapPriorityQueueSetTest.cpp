#include <gtest/gtest.h>
#include "runtime/state/heap/HeapPriorityQueueSet.h"

class IntType
{
private:
    int val;

public:
    IntType(int val) : val(val) {};
    int getKey() { return val; }
    size_t hashCode() { return std::hash<int>()(val); };

    bool operator==(const IntType other)
    {
        return this->val == other.val;
    }
    bool operator!=(const IntType other)
    {
        return !(this->val == other.val);
    }
};

// Custom hasher and equality operators
namespace std
{
    template <>
    struct hash<IntType *>
    {
        size_t operator()(IntType *obj) const
        {
            if (obj)
            {
                return obj->hashCode();
            }
            else
            {
                return 0;
            }
        }
    };

    template <>
    struct equal_to<IntType *>
    {
        bool operator()(IntType *lhs, IntType *rhs) const
        {
            return lhs->getKey() == rhs->getKey();
        }
    };
}

struct Comparator
{
    bool operator()(IntType *a, IntType *b) { return a->getKey() > b->getKey(); }
};

TEST(HeapPriorityQueueSetTest, Initialization)
{
    auto *keyGroupRange = new KeyGroupRange(1, 5);
    HeapPriorityQueueSet<IntType *, Comparator> *pq = new HeapPriorityQueueSet<IntType *, Comparator>(keyGroupRange, keyGroupRange->getNumberOfKeyGroups(), 10);
    pq->add<int>(new IntType(1));
    EXPECT_EQ(pq->poll<int>()->getKey(), 1);
}

TEST(HeapPriorityQueueSetTest, Inserts)
{
    auto *keyGroupRange = new KeyGroupRange(1, 5);
    HeapPriorityQueueSet<IntType *, Comparator> *pq = new HeapPriorityQueueSet<IntType *, Comparator>(keyGroupRange, keyGroupRange->getNumberOfKeyGroups(), 10);
    pq->add<int>(new IntType(1));
    pq->add<int>(new IntType(3));
    pq->add<int>(new IntType(2));
    pq->add<int>(new IntType(4));

    EXPECT_EQ(pq->poll<int>()->getKey(), 1);
    EXPECT_EQ(pq->poll<int>()->getKey(), 2);
    EXPECT_EQ(pq->poll<int>()->getKey(), 3);
    EXPECT_EQ(pq->poll<int>()->getKey(), 4);
}

TEST(HeapPriorityQueueSetTest, HeadChange)
{
    auto *keyGroupRange = new KeyGroupRange(1, 5);
    HeapPriorityQueueSet<IntType *, Comparator> *pq = new HeapPriorityQueueSet<IntType *, Comparator>(keyGroupRange, keyGroupRange->getNumberOfKeyGroups(), 10);
    EXPECT_EQ(pq->add<int>(new IntType(5)), true);
    EXPECT_EQ(pq->add<int>(new IntType(3)), true);
    EXPECT_EQ(pq->add<int>(new IntType(3)), false);
    EXPECT_EQ(pq->add<int>(new IntType(4)), false);
    EXPECT_EQ(pq->add<int>(new IntType(1)), true);
}

TEST(HeapPriorityQueueSetTest, ClearTest)
{
    auto *keyGroupRange = new KeyGroupRange(1, 5);
    HeapPriorityQueueSet<IntType *, Comparator> *pq = new HeapPriorityQueueSet<IntType *, Comparator>(keyGroupRange, keyGroupRange->getNumberOfKeyGroups(), 10);
    EXPECT_EQ(pq->add<int>(new IntType(5)), true);
    EXPECT_EQ(pq->isEmpty(), false);
    EXPECT_EQ(pq->poll<int>()->getKey(), 5);
    EXPECT_EQ(pq->poll<int>(), nullptr);
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST(HeapPriorityQueueSetTest, DuplicateTest)
{
    auto *keyGroupRange = new KeyGroupRange(1, 5);
    HeapPriorityQueueSet<IntType *, Comparator> *pq = new HeapPriorityQueueSet<IntType *, Comparator>(keyGroupRange, keyGroupRange->getNumberOfKeyGroups(), 10);
    pq->add<int>(new IntType(1));
    pq->add<int>(new IntType(1));

    EXPECT_EQ(pq->poll<int>()->getKey(), 1);
    EXPECT_EQ(pq->poll<int>(), nullptr);
}

TEST(HeapPriorityQueueSetTest, RemoveTest)
{
    auto *keyGroupRange = new KeyGroupRange(1, 5);
    HeapPriorityQueueSet<IntType *, Comparator> *pq = new HeapPriorityQueueSet<IntType *, Comparator>(keyGroupRange, keyGroupRange->getNumberOfKeyGroups(), 10);
    pq->add<int>(new IntType(1));
    pq->add<int>(new IntType(2));
    pq->add<int>(new IntType(3));

    pq->remove<int>(new IntType(2));
    EXPECT_EQ(pq->poll<int>()->getKey(), 1);
    EXPECT_EQ(pq->poll<int>()->getKey(), 3);
}