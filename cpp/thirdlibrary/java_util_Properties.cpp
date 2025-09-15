/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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
    path = str.substr(0,str.find_last_of('/') + 1);

    filePath = path.append("flinkMetrics.properties").data();


//    char *filePath = "/home/workspace/java/metrics-parser-job/src/main/resources/flinkMetrics.properties";
    std::ifstream file;
    file.open(filePath, std::ios::in);

    if (!file.is_open()) {
        return;
    }

    int limit;
    int keyLen;
    int valueStart = 0;
    char c;
    bool hasSep;
    bool precedingBackslash;

    std::string strLine;
    while (std::getline(file, strLine)) {
        if (strLine.empty() || strLine[0] == '#' || strLine[0] == '!') {
            continue;
        }
        if (strLine.back() == '\r') {
            strLine.erase(strLine.size() - 1);
        }
        c = 0;
        keyLen = 0;
        limit = strLine.size();
        hasSep = false;
        precedingBackslash = false;

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

        std::string key = strLine.substr(0, keyLen);
        std::string value = strLine.substr(valueStart, limit - valueStart);
        this->put(key, value);
    }
}