//
// Created by root on 12/30/25.
//

#ifndef OMNISTREAM_CHANNELSTATEHOLDER_H
#define OMNISTREAM_CHANNELSTATEHOLDER_H
#include "runtime/checkpoint/channel/ChannelStateWriter.h"
namespace omnistream {
    class ChannelStateHolder {
        public:
        virtual void setChannelStateWriter(std::shared_ptr<ChannelStateWriter> channelStateWriter) = 0;
    };
}
#endif //OMNISTREAM_CHANNELSTATEHOLDER_H
