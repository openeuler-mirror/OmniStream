/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/28/25.
//

#ifndef FLINK_TNEL_CALLBACK_H
#define FLINK_TNEL_CALLBACK_H

#include <jni.h>

class CallBack {
public:
    void SetArgs(jmethodID methodID, JNIEnv *env, jobject task) {
        this->methodID = methodID;
        this->env = env;
        this->task = task;
    }

    void process() {
        env->CallVoidMethod(task, methodID);
    }
private:
//    StreamTask *ptask;
    jmethodID methodID;
    JNIEnv *env;
    jobject task;
};
#endif //FLINK_TNEL_CALLBACK_H
