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

using PLS_MV = std::variant</*int, long, double, std::string, bool, */std::vector<uint8_t>>;
using BS_K_MV = std::variant<std::vector<uint8_t>>;
using BS_V_MV = std::variant<std::vector<uint8_t>>;

class CustomVariant {
public:
    template<typename V>
    static Object* VariantToObject(V& variant) {
        return std::visit([](auto& arg) -> Object * {
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
            } else if constexpr (std::is_same_v<T, std::vector<uint8_t> >) {
                int size = static_cast<int>(arg.size());
                if (size < 0) {
                    size = 0;
                    return new JavaArray<uint8_t>(size);
                }
                auto* arr = new JavaArray<uint8_t>(size);
                std::copy(arg.begin(), arg.end(), arr->data());
                return arr;
            } else {
                throw std::runtime_error("Unsupported variant type");
            }
        }, variant);
    }

    static Object* PLS_MVToObject(const PLS_MV& variant) {
        return CustomVariant::VariantToObject(variant);
    }

    static Object* BS_K_MVToObject(const BS_K_MV& variant) {
        return CustomVariant::VariantToObject(variant);
    }

    static Object* BS_V_MVToObject(const BS_V_MV&variant) {
        return CustomVariant::VariantToObject(variant);
    }
};

#endif //OMNISTREAMOTHER_VARIANT_H