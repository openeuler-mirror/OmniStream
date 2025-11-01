/*
* Copyright (c) Huawei Technologies Co., Ltd. 2012-2025. All rights reserved.
 * Create Date : 2025
 */

#ifndef OMNISTREAM_REMOTEDATAFETCHERBRIDGEIMPL_H
#define OMNISTREAM_REMOTEDATAFETCHERBRIDGEIMPL_H
#include <common/global.h>

#include "runtime/partition/consumer/RemoteDataFetcherBridge.h"

class RemoteDataFetcherBridgeImpl : public omnistream::RemoteDataFetcherBridge {
public:
    RemoteDataFetcherBridgeImpl() = default;

    ~RemoteDataFetcherBridgeImpl();

    void SetJavaRemoteDataFetcher(JNIEnv *env, jobject fetcher);

    void InvokeJavaRemoteDataFetcherResumeConsumption(int inputGateIndex, int channelIndex) override;

    void CleanJavaRemoteDataFetcher();

private:
    jobject javaRemoteDataFetcherRef_ = nullptr;
};
#endif // OMNISTREAM_REMOTEDATAFETCHERBRIDGEIMPL_H
