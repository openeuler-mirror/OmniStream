/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#ifdef WITH_OMNISTATESTORE

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>

#include "common.h"
#include "bss_err.h"

namespace bss_adapter {

template <typename Exception>
[[noreturn]] inline void ThrowWithLog(const std::string& message)
{
    ERROR_RELEASE(message);
    throw Exception(message);
}

inline void LogStateOperationSuccess(
    const char* stateType,
    const std::string& stateName,
    const char* operation,
    uint32_t keyHashCode,
    const std::string& outcome)
{
    INFO_RELEASE(
        "BSS state operation success, stateType=" << stateType << ", state=" << stateName
                                                   << ", operation=" << operation << ", keyHash=" << keyHashCode
                                                   << ", " << outcome);
}

inline bool IsNotFound(ock::bss::BResult result)
{
    return result == ock::bss::BSS_NOT_FOUND || result == ock::bss::BSS_NOT_EXISTS;
}

inline void CheckResult(ock::bss::BResult result, const std::string& operation)
{
    if (result != ock::bss::BSS_OK) {
        const std::string message =
            "OmniStateStore operation '" + operation + "' failed, error code=" + std::to_string(result);
        ThrowWithLog<std::runtime_error>(message);
    }
}

template <typename T>
inline void CheckTable(const std::shared_ptr<T>& table, const std::string& stateName)
{
    if (table == nullptr) {
        const std::string message = "OmniStateStore failed to create table for state '" + stateName + "'";
        ThrowWithLog<std::runtime_error>(message);
    }
}

} // namespace bss_adapter

#endif
