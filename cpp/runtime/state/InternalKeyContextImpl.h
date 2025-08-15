#ifndef FLINK_TNEL_INTERNALKEYCONTEXTIMPL_H
#define FLINK_TNEL_INTERNALKEYCONTEXTIMPL_H

#include "InternalKeyContext.h"
#include "KeyGroupRange.h"
#include "KeyGroupRangeAssignment.h"
#include <string>

template <typename K>
class InternalKeyContextImpl : public InternalKeyContext<K>
{
private:
    KeyGroupRange *keyGroupRange;
    int numberOfKeyGroups;
    K currentKey;
    int currentKeyGroupIndex;

public:
    InternalKeyContextImpl(KeyGroupRange *keyGroupRange, int numberOfKeyGroups) : keyGroupRange(keyGroupRange), numberOfKeyGroups(numberOfKeyGroups) {
        if constexpr (std::is_same_v<K, Object*>) {
            currentKey = nullptr;
        } else if constexpr (std::is_same_v<K, int64_t>) {
            // Initialize currentKey and currentKeyGroupIndex
            currentKey = 0;
            setCurrentKey(currentKey);
        } else if constexpr (std::is_pointer_v<K>) {
            // Initialize currentKey and currentKeyGroupIndex
            currentKey = nullptr;
        }
    };

    ~InternalKeyContextImpl() override = default;

    // Getters
    K getCurrentKey() override { return currentKey; };
    int getCurrentKeyGroupIndex() override { return currentKeyGroupIndex; };
    int getNumberOfKeyGroups() override { return numberOfKeyGroups; };
    KeyGroupRange *getKeyGroupRange() override { return keyGroupRange; };

    // Setters
    void setCurrentKey(K currentKey) override {
        this->currentKey = currentKey;
        setCurrentKeyGroupIndex(KeyGroupRangeAssignment<K>::assignToKeyGroup(this->currentKey, getNumberOfKeyGroups()));
    };

    void setCurrentKeyGroupIndex(int currentKeyGroupIndex) override;
};
template <typename K>
inline void InternalKeyContextImpl<K>::setCurrentKeyGroupIndex(int currentKeyGroupIndex) {
    if (!keyGroupRange->contains(currentKeyGroupIndex))
    {
        std::string err = "Key group " + std::to_string(currentKeyGroupIndex) + " is not in the range of " + std::to_string(keyGroupRange->getStartKeyGroup()) + " and " + std::to_string(keyGroupRange->getEndKeyGroup());
        THROW_LOGIC_EXCEPTION(err);
    }
    this->currentKeyGroupIndex = currentKeyGroupIndex;
}
#endif // FLINK_TNEL_INTERNALKEYCONTEXTIMPL_H
