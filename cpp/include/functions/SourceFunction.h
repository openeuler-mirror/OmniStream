/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef SOURCEFUNCTION_H
#define SOURCEFUNCTION_H

#include <type_traits>

#include "SourceContext.h"
#include "basictypes/callback.h"

template <typename T>
class SourceFunction : public Object {
    // Static assertion for compile-time check (preferred)
    // static_assert(std::is_base_of_v<StreamRecord, T>, "T must be derived from StreamRecord");

public:
    virtual void run(SourceContext* context) = 0;
    virtual void cancel() = 0;

    // Intermediate operation, which can be deleted after dataStream shuffle is nativeized.
    void SaveCallBack(CallBack *callback) {
        this->callback = callback;
    }
    ~SourceFunction() {
        if (callback != nullptr) {
            delete callback;
        }
    }

protected:
    CallBack *callback = nullptr;
};

template<typename T>
using SourceFunctionUnique = std::unique_ptr<SourceFunction<T>>;


#endif  // SOURCEFUNCTION_H
