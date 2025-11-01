/*
* Copyright (c) Huawei Technologies Co., Ltd. 2012-2025. All rights reserved.
 * Create Date : 2025
 */

#ifndef OMNISTREAM_OMNILOCALINPUTCHANNELBRIDGEIMPL_H
#define OMNISTREAM_OMNILOCALINPUTCHANNELBRIDGEIMPL_H

#include "jni/common/global.h"
#include "runtime/state/bridge/OmniLocalInputChannelBridge.h"

class OmniLocalInputChannelBridgeImpl : public omnistream::OmniLocalInputChannelBridge {
public:
    ~OmniLocalInputChannelBridgeImpl();

    void RegisterJavaOmniLocalInputChannel(JNIEnv *env, jobject javaRemoteDataFetcher);

    void ClearJavaChannelRef();

    void InvokeDoResumeConsumption() override; // call Java doResumeConsumption()
private:
    jobject javaOmniLocalInputChannel = nullptr;
};
#endif // OMNISTREAM_OMNILOCALINPUTCHANNELBRIDGEIMPL_H
