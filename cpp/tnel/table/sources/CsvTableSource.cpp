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

#include "CsvTableSource.h"

size_t CsvTableSource::countCsvRows()
{
    std::ifstream file(filepath, std::ios::binary);
    if (!file) {
        LOG("Error opening file: " + filepath);
        return 0;
    }
    size_t line_count = 0;
    char buffer[8192];

    while (file.read(buffer, sizeof(buffer))) {
        line_count += static_cast<size_t>(std::count(buffer, buffer + file.gcount(), '\n'));
    }
    // Count remaining characters in the last read
    line_count += static_cast<size_t>(std::count(buffer, buffer + file.gcount(), '\n'));

    return line_count;
}
