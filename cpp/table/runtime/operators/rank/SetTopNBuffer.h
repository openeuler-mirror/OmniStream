#pragma once
#include <set>

/**
 * In-memory ordered TopN buffer.
 * Stores comboIds (long), sorted by Comparator.
 * Owns the std::set value directly (no raw pointer ownership problems).
 */
template <typename Comparator>
class SetTopNBuffer {
public:
    using Buffer = std::set<long, Comparator>;

    explicit SetTopNBuffer(Comparator cmp)
        : buffer_(cmp),bufferId(-99) {}

    inline auto begin() { return buffer_.begin(); }
    inline auto end()   { return buffer_.end(); }

    inline void AddElement(long id) { buffer_.insert(id); }

    inline void RemoveSmallestElement() {
        if (!buffer_.empty()) buffer_.erase(buffer_.begin());
    }

    inline int GetSize() const { return (int)buffer_.size(); }

    inline long GetSmallestElement() const { return *buffer_.begin(); }

    inline void LoadFromPlainSet(const std::set<long>& plain) {
        buffer_.clear();
        buffer_.insert(plain.begin(), plain.end());
    }

    inline std::set<long>* ToPlainSet() const {
        return new std::set<long>(buffer_.begin(), buffer_.end());
    }
    inline void SetBufferId(int id) { bufferId = id; }
    inline int GetBufferId() const { return bufferId; }

private:
    int bufferId;// unique buffer identifier
    Buffer buffer_;
};
