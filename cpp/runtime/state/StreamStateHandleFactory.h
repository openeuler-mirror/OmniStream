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
#ifndef OMNISTREAM_STREAMSTATEHANDLEFACTORY_H
#define OMNISTREAM_STREAMSTATEHANDLEFACTORY_H

#include <memory>
#include "runtime/state/StreamStateHandle.h"
#include "runtime/state/filesystem/FileStateHandle.h"
#include "state/filesystem/RelativeFileStateHandle.h"
#include "state/memory/ByteStreamStateHandle.h"
#include <nlohmann/json.hpp>

class StreamStateHandleFactory {
public:
    static std::shared_ptr<StreamStateHandle> from_json(const nlohmann::json& json)
    {
        if (!json.contains("@class")) {
            throw std::runtime_error("metadata State JSON is missing the '@class' field.");
        }
        const std::string className = json.at("@class").get<std::string>();
        if (className.find("RelativeFileStateHandle") != std::string::npos) {
            Path filePath(json["streamStateHandleID"]["keyString"].get<std::string>());
            long stateSize = json["stateSize"].get<long>();
            return std::make_shared<RelativeFileStateHandle>(filePath, json["relativePath"], stateSize);
        } else if (className.find("FileStateHandle") != std::string::npos) {
            long stateSize = json["stateSize"].get<long>();
            if (json.contains("filePath")) {
                Path filePath(json["filePath"].get<std::string>());
                return std::make_shared<FileStateHandle>(filePath, stateSize);
            } else {
                Path filePath(json["streamStateHandleID"]["keyString"].get<std::string>());
                return std::make_shared<FileStateHandle>(filePath, stateSize);
            }
        } else if (className.find("ByteStreamStateHandle") != std::string::npos) {
            std::string handleName = json["handleName"].get<std::string>();
            std::string encoded_data = json.at("data").get<std::string>();
            return std::make_shared<ByteStreamStateHandle>(handleName, Base64_decode(encoded_data));
        }
        return nullptr;
    }
};

#endif // OMNISTREAM_STREAMSTATEHANDLEFACTORY_H