#ifndef OMNISTREAM_VOID_H
#define OMNISTREAM_VOID_H

#include "Object.h"

class Void : public Object {
public:
    Void() = default;

    Void(const Void& other) = default;

    ~Void() override = default;

    Void& operator=(const Void& other) { return *this; }

    bool operator==(const Void &other) const { return true; }

    operator size_t() const { return 0; }

    operator int64_t() const { return 0; }

    int hashCode() {  return 99; }
};

namespace std {
    template <>
    struct hash<Void> {
        std::size_t operator()(const Void &ns) const noexcept {
            return 99;
        }
    };

    template <>
    struct equal_to<Void> {
        bool operator()(const Void &lhs, const Void &rhs) const noexcept {
            return true;
        }
    };
}

#endif //OMNISTREAM_VOID_H