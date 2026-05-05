/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef OMNISTREAMOTHER_VARIANT_H
#define OMNISTREAMOTHER_VARIANT_H

#include <variant>
#include <memory>
#include <vector>
#include <iostream>
#include <string>
#include "basictypes/Long.h"
#include "basictypes/Double.h"
#include "basictypes/String.h"
#include "basictypes/JavaArray.h"

using STATE_MV = std::variant<int, long, double, std::string, bool, std::vector<uint8_t>>;


static Object* variantToObject(const STATE_MV& variant) {
 return std::visit([](auto&& arg) -> Object* {
     using T = std::decay_t<decltype(arg)>;
     if constexpr (std::is_same_v<T, int>) {
         return new Long(static_cast<int64_t>(arg));
     } else if constexpr (std::is_same_v<T, long>) {
         return new Long(arg);
     } else if constexpr (std::is_same_v<T, double>) {
         return new Double(arg);
     } else if constexpr (std::is_same_v<T, std::string>) {
         return new String(arg);
     } else if constexpr (std::is_same_v<T, bool>) {
         return new Long(arg ? 1 : 0);
     } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
         auto* arr = new JavaArray<uint8_t>(static_cast<int>(arg.size()));
         std::copy(arg.begin(), arg.end(), arr->data());
         return arr;
     } else {
         return nullptr;
     }
 }, variant);
}
#endif //OMNISTREAMOTHER_VARIANT_H
