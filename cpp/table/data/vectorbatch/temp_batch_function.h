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

#ifndef OMNISTREAM_TEMP_BATCH_FUNCTION_H
#define OMNISTREAM_TEMP_BATCH_FUNCTION_H
#include <ctime>
#include <iomanip>
#include <sstream>
#include <regex>
#include <string>
#include "VectorBatch.h"
#include "core/include/common.h"
using namespace omniruntime::vec;
namespace omnistream {
    // for DATE_FORMAT
    BaseVector* BatchDateFormat(BaseVector* inputVec, const std::string &format);
    BaseVector* RegexpExtract(BaseVector* inputVec, std::string& regexToMatch, int32_t group);
}


#endif
