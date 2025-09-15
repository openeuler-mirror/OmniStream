#ifndef FLINK_TNEL_ROWKIND_H
#define FLINK_TNEL_ROWKIND_H

#include <cstdint>

enum class RowKind : std::uint8_t {
    INSERT = 0,
    UPDATE_BEFORE = 1,
    UPDATE_AFTER = 2,
    DELETE = 3
};

inline const char* to_string(RowKind kind) {
    switch (kind) {
        case RowKind::INSERT: return "+I";
        case RowKind::UPDATE_BEFORE: return "-U";
        case RowKind::UPDATE_AFTER: return "+U";
        case RowKind::DELETE: return "-D";
        default: return "UNKNOWN"; // 处理未定义的枚举值
    }
}

#endif //FLINK_TNEL_ROWKIND_H
