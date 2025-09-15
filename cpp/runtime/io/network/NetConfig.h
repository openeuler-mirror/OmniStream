/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef NETCONFIG_H
#define NETCONFIG_H

#include <string>
#include <sstream>

namespace omnistream {

    class NetConfig {
    public:
        static const std::string SERVER_THREAD_GROUP_NAME;
        static const std::string CLIENT_THREAD_GROUP_NAME;

        NetConfig();
        NetConfig(const std::string& serverAddress, int serverPort);
        ~NetConfig();

        std::string getServerAddress() const;
        void setServerAddress(const std::string& serverAddress);

        int getServerPort() const;
        void setServerPort(int serverPort);

        std::string toString() const;

    private:
        std::string serverAddress;
        int serverPort;
    };

} // namespace omnistream

#endif // NETCONFIG_H