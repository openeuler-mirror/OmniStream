/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd
 * OmniStream is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "AvailabilityProvider.h"
#include <sstream>

namespace omnistream {

std::shared_ptr<CompletableFuture> AvailabilityProvider::AVAILABLE = std::make_shared<CompletableFuture>(true);

std::shared_ptr<CompletableFuture> AvailabilityProvider::and_(
    std::shared_ptr<CompletableFuture> first, std::shared_ptr<CompletableFuture> second)
{
    if (first == AVAILABLE && second == AVAILABLE) {
        return AVAILABLE;
    } else if (first == AVAILABLE) {
        return second;
    } else if (second == AVAILABLE) {
        return first;
    } else {
        return CompletableFuture::allOf(std::vector<std::shared_ptr<CompletableFuture>>{first, second});
    }
}

std::shared_ptr<CompletableFuture> AvailabilityProvider::or_(
    std::shared_ptr<CompletableFuture> first, std::shared_ptr<CompletableFuture> second)
{
    if (first == AVAILABLE || second == AVAILABLE) {
        return AVAILABLE;
    }
    return CompletableFuture::anyOf(std::vector<std::shared_ptr<CompletableFuture>>{first, second});
}

} // namespace omnistream
