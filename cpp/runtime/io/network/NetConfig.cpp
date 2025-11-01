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

#include "NetConfig.h"

namespace omnistream {

    const std::string NetConfig::SERVER_THREAD_GROUP_NAME = "Flink Net Server";
    const std::string NetConfig::CLIENT_THREAD_GROUP_NAME = "Flink Net Client";

    NetConfig::NetConfig() : serverAddress(""), serverPort(0) {}

    NetConfig::NetConfig(const std::string& serverAddress, int serverPort)
        : serverAddress(serverAddress), serverPort(serverPort) {}

    NetConfig::~NetConfig() {}

    std::string NetConfig::getServerAddress() const
    {
        return serverAddress;
    }

    void NetConfig::setServerAddress(const std::string& serverAddress_)
    {
        this->serverAddress = serverAddress_;
    }

    int NetConfig::getServerPort() const
    {
        return serverPort;
    }

    void NetConfig::setServerPort(int serverPort_)
    {
        this->serverPort = serverPort_;
    }

    std::string NetConfig::toString() const
    {
        std::stringstream ss;
        ss << "NetConfig{"
           << "serverAddress='" << serverAddress << '\''
           << ", serverPort=" << serverPort
           << '}';
        return ss.str();
    }

} // namespace omnistream