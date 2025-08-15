/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef EVENT_TIME_TRIGGERS_H
#define EVENT_TIME_TRIGGERS_H

#include <string>
#include "core/api/common/state/ValueStateDescriptor.h"
#include "WindowTrigger.h"
#include "Trigger.h"

template<typename W>
class EventTimeTriggers {
public:
    class AfterEndOfWindowEarlyAndLate : public WindowTrigger<W> {
    public:
        AfterEndOfWindowEarlyAndLate(Trigger<W> *earlyTrigger, Trigger<W> *lateTrigger): earlyTrigger(earlyTrigger),
            lateTrigger(lateTrigger), hasFiredOnTimeStateDesc("eventTime-afterEOW", (TypeSerializer*) nullptr) {}

        void Open(typename Trigger<W>::TriggerContext *ctx) override
        {
            this->ctx = ctx;
            if (earlyTrigger != nullptr) {
                earlyTrigger->Open(ctx);
            }
            if (lateTrigger != nullptr) {
                lateTrigger->Open(ctx);
            }
        }

        bool OnElement(RowData *element, long timestamp, W window) override
        {
            bool hasFired = this->ctx->GetPartitionedState(hasFiredOnTimeStateDesc).value();
            if (hasFired) {
                // this is to cover the case where we recover from a failure and the watermark
                // is Long.MIN_VALUE but the window is already in the late phase.
                return lateTrigger != nullptr && lateTrigger->OnElement(element, timestamp, window);
            } else {
                if (triggerTime(window) <= this->ctx.getCurrentWatermark()) {
                    // we are in the late phase

                    // if there is no late trigger then we fire on every late element
                    // This also covers the case of recovery after a failure
                    // where the currentWatermark will be Long.MIN_VALUE
                    return true;
                } else {
                    // we are in the early phase
                    this->ctx->RegisterEventTimeTimer(triggerTime(window));
                    return earlyTrigger != nullptr && earlyTrigger->OnElement(element, timestamp, window);
                }
            }
        }

        bool OnProcessingTime(long time, W window) override
        {
            bool hasFired = this->ctx->GetPartitionedState(hasFiredOnTimeStateDesc).value();
            if (hasFired) {
                // late fire
                return lateTrigger != nullptr && lateTrigger->OnProcessingTime(time, window);
            } else {
                // early fire
                return earlyTrigger != nullptr && earlyTrigger->OnProcessingTime(time, window);
            }
        }

        bool OnEventTime(long time, W window)
        {
            ValueState<bool> *hasFiredState = this->ctx->GetPartitionedState(hasFiredOnTimeStateDesc);
            bool hasFired = hasFiredState->value();
            if (hasFired) {
                // late fire
                return lateTrigger != nullptr && lateTrigger->OnEventTime(time, window);
            } else {
                if (time == triggerTime(window)) {
                    // fire on time and update state
                    hasFiredState->update(true);
                    return true;
                } else {
                    // early fire
                    return earlyTrigger != nullptr && earlyTrigger->OnEventTime(time, window);
                }
            }
        }

        bool CanMerge() override
        {
            return (earlyTrigger == nullptr || earlyTrigger->CanMerge())
                    && (lateTrigger == nullptr || lateTrigger->CanMerge());
        }

        void OnMerge(W window, OnMergeContext<W> *mergeContext) override
        {
            if (earlyTrigger != nullptr) {
                earlyTrigger->OnMerge(window, mergeContext);
            }
            if (lateTrigger != nullptr) {
                lateTrigger->OnMerge(window, mergeContext);
            }

            // we assume that the new merged window has not fired yet its on-time timer.
            this->ctx->GetPartitionedState(hasFiredOnTimeStateDesc).update(false);
            this->ctx->RegisterEventTimeTimer(triggerTime(window));
        }

        void Clear(W window) override
        {
            if (earlyTrigger != nullptr) {
                earlyTrigger->Clear(window);
            }
            if (lateTrigger != nullptr) {
                lateTrigger->Clear(window);
            }
            this->ctx->DeleteEventTimeTimer(triggerTime(window));
            this->ctx->GetPartitionedState(hasFiredOnTimeStateDesc).clear();
        }

    private:
        Trigger<W> *earlyTrigger;
        Trigger<W> *lateTrigger;
        ValueStateDescriptor hasFiredOnTimeStateDesc;
    };

    class AfterEndOfWindow : public WindowTrigger<W> {
    public:
        AfterEndOfWindow() = default;

        void Open(typename Trigger<W>::TriggerContext *ctx) override
        {
            this->ctx = ctx;
        }

        bool OnElement(RowData *element, long timestamp, W window)
        {
            if (this->TriggerTime(window) <= this->ctx->GetCurrentWatermark()) {
                // if the watermark is already past the window fire immediately
                return true;
            } else {
                // todo 方法未实现？
                this->ctx->RegisterEventTimeTimer(this->TriggerTime(window));
                return false;
            }
        }

        bool OnProcessingTime(long time, W window) override
        {
            return false;
        }

        bool OnEventTime(long time, W window) override
        {
            return time == this->TriggerTime(window);
        }

        void Clear(W window) override
        {
            // todo 没有实现清理internalTimerService对应event队列元素功能
            this->ctx->DeleteEventTimeTimer(this->TriggerTime(window));
        }
    };
};

#endif // EVENT_TIME_TRIGGERS_H
