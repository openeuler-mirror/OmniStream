/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "NetConfig.h"

namespace omnistream {

    const std::string NetConfig::SERVER_THREAD_GROUP_NAME = "Flink Net Server";
    const std::string NetConfig::CLIENT_THREAD_GROUP_NAME = "Flink Net Client";

    NetConfig::NetConfig() : serverAddress(""), serverPort(0) {}

    NetConfig::NetConfig(const std::string& serverAddress, int serverPort)
        : serverAddress(serverAddress), serverPort(serverPort) {}

    NetConfig::~NetConfig() {}

    std::string NetConfig::getServerAddress() const {
        return serverAddress;
    }

    void NetConfig::setServerAddress(const std::string& serverAddress) {
        this->serverAddress = serverAddress;
    }

    int NetConfig::getServerPort() const {
        return serverPort;
    }

    void NetConfig::setServerPort(int serverPort) {
        this->serverPort = serverPort;
    }

    std::string NetConfig::toString() const {
        std::stringstream ss;
        ss << "NetConfig{"
           << "serverAddress='" << serverAddress << '\''
           << ", serverPort=" << serverPort
           << '}';
        return ss.str();
    }

} // namespace omnistream