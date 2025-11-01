/*
* Copyright (c) Huawei Technologies Co., Ltd. 2012-2025. All rights reserved.
 * Create Date : 2025
 */
#include "RemoteDataFetcherBridgeImpl.h"

RemoteDataFetcherBridgeImpl::~RemoteDataFetcherBridgeImpl()
{
    CleanJavaRemoteDataFetcher();
}

void RemoteDataFetcherBridgeImpl::SetJavaRemoteDataFetcher(JNIEnv *env, jobject fetcher)
{
    if (javaRemoteDataFetcherRef_) {
        env->DeleteGlobalRef(javaRemoteDataFetcherRef_);
        javaRemoteDataFetcherRef_ = nullptr;
    }
    if (fetcher) {
        javaRemoteDataFetcherRef_ = env->NewGlobalRef(fetcher);
    }
}

void RemoteDataFetcherBridgeImpl::InvokeJavaRemoteDataFetcherResumeConsumption(int inputGateIndex, int channelIndex)
{
    if (!g_OmniStreamJVM || !javaRemoteDataFetcherRef_) {
        return;
    }

    JNIEnv *env = nullptr;
    bool attached = false;

    if (g_OmniStreamJVM->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8) != JNI_OK) {
        if (g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void **>(&env), nullptr) != 0) {
            return;
        }
        attached = true;
    }

    jclass cls = env->GetObjectClass(javaRemoteDataFetcherRef_);
    if (!cls) {
        if (attached) g_OmniStreamJVM->DetachCurrentThread();
        return;
    }
    jmethodID mid = env->GetMethodID(cls, "doResumeConsumption", "(II)V");

    env->DeleteLocalRef(cls);
    if (!mid) {
        if (attached) g_OmniStreamJVM->DetachCurrentThread();
        return;
    }
    env->CallVoidMethod(javaRemoteDataFetcherRef_, mid, (jint) inputGateIndex, (jint) channelIndex);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
    }

    if (attached) {
        g_OmniStreamJVM->DetachCurrentThread();
    }
}

void RemoteDataFetcherBridgeImpl::CleanJavaRemoteDataFetcher()
{
    if (!g_OmniStreamJVM || !javaRemoteDataFetcherRef_) {
        return;
    }
    JNIEnv *env = nullptr;
    bool attached = false;
    if (g_OmniStreamJVM->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8) != JNI_OK) {
        if (g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void **>(&env), nullptr) == 0) {
            attached = true;
        } else {
            return;
        }
    }
    if (javaRemoteDataFetcherRef_) {
        env->DeleteGlobalRef(javaRemoteDataFetcherRef_);
        javaRemoteDataFetcherRef_ = nullptr;
    }
    if (attached) {
        g_OmniStreamJVM->DetachCurrentThread();
    }
}
