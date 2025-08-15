//
// Created by root on 3/3/25.
//

#ifndef OMNISTREAM_OMNISTREAMTASKINPUT_H
#define OMNISTREAM_OMNISTREAMTASKINPUT_H

#include "OmniPushingAsyncDataInput.h"
namespace omnistream {
    class OmniStreamTaskInput : public OmniPushingAsyncDataInput {
    public:
        virtual int getInputIndex() = 0;

        virtual void close() {};
    };

}

#endif //OMNISTREAM_OMNISTREAMTASKINPUT_H
