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