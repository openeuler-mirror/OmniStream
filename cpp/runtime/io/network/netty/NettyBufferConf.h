/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#ifndef NETTY_BUFFER_CONF_H
#define NETTY_BUFFER_CONF_H

namespace omnistream {

class NettyBufferConf {
public:
    int totalPoolSize;
    int bufferSize;
    int configuredBufferPerChannel;
    int numOfFloatingBufferPerGate;

    NettyBufferConf(int totalPoolSize = 1000,
                    int bufferSize = 32 * 1024,
                    int numRequiredBuffersPerPool = 2,
                    int maxBuffersPerPool = 8)
        : totalPoolSize(totalPoolSize),
          bufferSize(bufferSize),
          configuredBufferPerChannel(numRequiredBuffersPerPool),
          numOfFloatingBufferPerGate(maxBuffersPerPool) {}
};

} // namespace omnistream

#endif // NETTY_BUFFER_CONF_H
