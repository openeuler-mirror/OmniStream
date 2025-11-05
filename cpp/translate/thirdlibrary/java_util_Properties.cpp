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

#include "thirdlibrary/java_util_Properties.h"
#include <iostream>
#include <fstream>

java_util_Properties::java_util_Properties() = default;
java_util_Properties::~java_util_Properties() = default;

int lib_fun()
{
    return 0;
}

void java_util_Properties::load(InputStream *input)
{
    Dl_info dl_info;
    dladdr((void*) lib_fun, &dl_info);
    static const char* filePath = nullptr;
    std::string path;
    std::string str = dl_info.dli_fname;
    path = str.substr(0, str.find_last_of('/') + 1);

    filePath = path.append("flinkMetrics.properties").data();

    std::ifstream file;
    file.open(filePath, std::ios::in);

    if (!file.is_open()) {
        return;
    }

    std::string strLine;
    while (std::getline(file, strLine)) {
        processLine(strLine);
    }
}

void java_util_Properties::processLine(std::string& strLine)
{
    if (strLine.empty() || strLine[0] == '#' || strLine[0] == '!') {
        return;
    }
    if (strLine.back() == '\r') {
        strLine.erase(strLine.size() - 1);
    }

    std::string key;
    std::string value;
    parseKeyValueFromLine(strLine, key, value);

    if (!key.empty()) {
        this->put(key, value);
    }
}

void java_util_Properties::parseKeyValueFromLine(std::string& strLine, std::string& outKey, std::string& outValue)
{
    size_t limit = strLine.size();
    size_t keyLen = 0;
    size_t valueStart = 0;
    bool hasSep = false;
    bool precedingBackslash = false;
    char c = 0;

    while (keyLen < limit) {
        c = strLine[keyLen];
        if ((c == '=' || c == ':') && !precedingBackslash) {
            valueStart = keyLen + 1;
            hasSep = true;
            break;
        } else if ((c == ' ' || c == '\t' || c == '\f') && !precedingBackslash) {
            valueStart = keyLen +1;
            break;
        }
        if (c == '\\') {
            precedingBackslash = !precedingBackslash;
        } else {
            precedingBackslash = false;
        }
        keyLen++;
    }

    while (valueStart < limit) {
        c = strLine[valueStart];
        if (c != ' ' && c != '\t' && c != '\f') {
            if (!hasSep && (c == '=' || c == ':')) {
                hasSep = true;
            } else {
                break;
            }
        }
        valueStart++;
    }
    outKey = strLine.substr(0, keyLen);
    outValue = strLine.substr(valueStart, limit - valueStart);
}