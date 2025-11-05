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
#ifndef TRIGGER_H
#define TRIGGER_H

#pragma once

template<typename W>
class OnMergeContext;

template<typename W>
class Trigger {
public:
    virtual ~Trigger() = default;

    class TriggerContext {
    public:
        virtual ~TriggerContext() = default;

        virtual long GetCurrentProcessingTime() = 0;

        virtual long GetCurrentWatermark() = 0;

        virtual void RegisterProcessingTimeTimer(long time) = 0;

        virtual void RegisterEventTimeTimer(long time) = 0;

        virtual void DeleteProcessingTimeTimer(long time) = 0;

        virtual void DeleteEventTimeTimer(long time) = 0;
    };

    virtual void Open(TriggerContext *ctx) = 0;

    virtual bool OnElement(RowData *element, long timestamp, W window) = 0;

    virtual bool OnProcessingTime(long time, W window) = 0;

    virtual bool OnEventTime(long time, W window) = 0;

    virtual bool CanMerge()
    {
        return false;
    }

    virtual void OnMerge(W window, OnMergeContext<W> *mergeContext)
    {
        throw std::runtime_error("This trigger does not support merging.");
    }

    virtual void Clear(W window) = 0;
};

template<typename W>
class OnMergeContext : public Trigger<W>::TriggerContext {};

#endif
