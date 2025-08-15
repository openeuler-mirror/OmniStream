/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_PROGRESSIVETIMESTAMPSANDWATERMARKS_H
#define OMNISTREAM_PROGRESSIVETIMESTAMPSANDWATERMARKS_H

#include <utility>
#include "TimestampsAndWatermarks.h"
#include "core/api/common/eventtime/WatermarkOutputMultiplexer.h"
#include "SourceOutputWithWatermarks.h"
#include "runtime/tasks/ScheduledFutureTask.h"

class ProgressiveTimestampsAndWatermarks :
    public TimestampsAndWatermarks, std::enable_shared_from_this<ProgressiveTimestampsAndWatermarks> {
public:
    ProgressiveTimestampsAndWatermarks(TimestampAssigner* timestampAssigner,
                                       std::shared_ptr<WatermarkGeneratorSupplier> watermarksFactory,
                                       ProcessingTimeService* timeService, long periodicWatermarkInterval);

    class IdlenessManager {
    public:
        // 内部 IdlenessAwareWatermarkOutput 类
        class IdlenessAwareWatermarkOutput : public WatermarkOutput {
        public:
            IdlenessAwareWatermarkOutput(WatermarkOutput* output, IdlenessManager* parent)
                : underlyingOutput_(output), isIdle_(true), parentManager_(parent) {}

            ~IdlenessAwareWatermarkOutput() override = default;
            void emitWatermark(Watermark* watermark) override
            {
                underlyingOutput_->emitWatermark(watermark);
                isIdle_ = false;
            }

            void MarkIdle() override
            {
                isIdle_ = true;
                parentManager_->CheckAndMarkIdle();
            }

            void MarkActive() override
            {
                isIdle_ = false;
                underlyingOutput_->MarkActive();
            }

            [[nodiscard]] bool IsIdle() const { return isIdle_; }

        private:
            WatermarkOutput* underlyingOutput_;
            bool isIdle_;
            IdlenessManager* parentManager_;  // 指向父管理器的指针
        };

        explicit IdlenessManager(WatermarkOutput* output) : underlyingOutput_(output),
            splitLocalOutput_(new IdlenessAwareWatermarkOutput(output, this)),
            mainOutput_(new IdlenessAwareWatermarkOutput(output, this)) {}

        ~IdlenessManager()
        {
            delete underlyingOutput_;
            delete splitLocalOutput_;
            delete mainOutput_;
        }

        IdlenessAwareWatermarkOutput* GetSplitLocalOutput()
        {
            return splitLocalOutput_;
        }

        IdlenessAwareWatermarkOutput* GetMainOutput()
        {
            return mainOutput_;
        }
    private:
        WatermarkOutput* underlyingOutput_;
        IdlenessAwareWatermarkOutput* splitLocalOutput_;
        IdlenessAwareWatermarkOutput* mainOutput_;

        void CheckAndMarkIdle()
        {
            if (splitLocalOutput_->IsIdle() && mainOutput_->IsIdle()) {
                underlyingOutput_->MarkIdle();
            }
        }
    };

    class SplitLocalOutputs {
    public:
        SplitLocalOutputs(OmniDataOutputPtr recordOutput, WatermarkOutput* watermarkOutput,
            TimestampAssigner* timestampAssigner, std::shared_ptr<WatermarkGeneratorSupplier> watermarksFactory)
            : recordOutput(recordOutput), timestampAssigner(timestampAssigner), watermarksFactory(watermarksFactory)
        {
            watermarkMultiplexer = new WatermarkOutputMultiplexer(watermarkOutput);
        }

        ~SplitLocalOutputs()
        {
            delete watermarkMultiplexer;
            for (auto& pair : localOutputs) {
                delete pair.second;
            }
        }

        // 禁止拷贝
        SplitLocalOutputs(const SplitLocalOutputs&) = delete;
        SplitLocalOutputs& operator=(const SplitLocalOutputs&) = delete;

        // 允许移动
        SplitLocalOutputs(SplitLocalOutputs&&) = default;
        SplitLocalOutputs& operator=(SplitLocalOutputs&&) = default;

        SourceOutputWithWatermarks* CreateOutputForSplit(const std::string& splitId)
        {
            auto it = localOutputs.find(splitId);
            if (it != localOutputs.end()) {
                return it->second;
            }

            watermarkMultiplexer->RegisterNewOutput(splitId);
            WatermarkOutput* onEventOutput = watermarkMultiplexer->GetImmediateOutput(splitId);
            WatermarkOutput* periodicOutput = watermarkMultiplexer->GetDeferredOutput(splitId);

            auto watermarks = watermarksFactory->CreateWatermarkGenerator();

            auto localOutput = SourceOutputWithWatermarks::createWithSeparateOutputs(recordOutput, onEventOutput,
                periodicOutput, timestampAssigner, watermarks);

            localOutputs.emplace(splitId, localOutput);
            return localOutput;
        }

        void ReleaseOutputForSplit(const std::string& splitId)
        {
            auto it = localOutputs.find(splitId);
            if (it == localOutputs.end()) {
                return;
            }
            SourceOutputWithWatermarks* value = it->second;
            localOutputs.erase(it);
            watermarkMultiplexer->UnregisterOutput(splitId);
            delete value;
        }

        void EmitPeriodicWatermark()
        {
            for (auto& pair : localOutputs) {
                pair.second->EmitPeriodicWatermark();
            }
            watermarkMultiplexer->OnPeriodicEmit();
        }

    private:
        WatermarkOutputMultiplexer* watermarkMultiplexer = nullptr;
        std::unordered_map<std::string, SourceOutputWithWatermarks*> localOutputs;
        OmniDataOutputPtr recordOutput;
        TimestampAssigner* timestampAssigner;
        std::shared_ptr<WatermarkGeneratorSupplier> watermarksFactory;
    };

    class StreamingReaderOutput : public SourceOutputWithWatermarks {
    public:
        StreamingReaderOutput(OmniDataOutputPtr output, WatermarkOutput* watermarkOutput,
            TimestampAssigner* timestampAssigner, WatermarkGenerator* watermarkGenerator,
            SplitLocalOutputs* splitLocalOutputs) : SourceOutputWithWatermarks(output, watermarkOutput,
            watermarkOutput, timestampAssigner, watermarkGenerator), splitLocalOutputs(splitLocalOutputs) {}

        SourceOutput& CreateOutputForSplit(const std::string& splitId) override
        {
            return *(splitLocalOutputs->CreateOutputForSplit(splitId));
        }

        void ReleaseOutputForSplit(const std::string& splitId) override
        {
            splitLocalOutputs->ReleaseOutputForSplit(splitId);
        }
    private:
        SplitLocalOutputs* splitLocalOutputs;
    };

    class TriggerProcessingTimeCallback : public ProcessingTimeCallback {
    public:
        explicit TriggerProcessingTimeCallback(std::shared_ptr<ProgressiveTimestampsAndWatermarks> eventTimeLogic)
            : eventTimeLogic(eventTimeLogic) {}

        ~TriggerProcessingTimeCallback() override = default;

        void OnProcessingTime(int64_t timestamp) override
        {
            eventTimeLogic->TriggerPeriodicEmit(timestamp);
        }

    private:
        std::shared_ptr<ProgressiveTimestampsAndWatermarks> eventTimeLogic;
    };

    TriggerProcessingTimeCallback* callback = nullptr;

    ~ProgressiveTimestampsAndWatermarks() override
    {
        delete timestampAssigner;
        delete currentPerSplitOutputs;
        delete currentMainOutput;
        delete idlenessManager;
        delete callback;
    }

    ReaderOutput* CreateMainOutput(
            OmniDataOutputPtr output, WatermarkUpdateListener* watermarkCallback) override;
    void StartPeriodicWatermarkEmits() override;
    void StopPeriodicWatermarkEmits() override;

private:
    TimestampAssigner* timestampAssigner = nullptr;
    std::shared_ptr<WatermarkGeneratorSupplier> watermarksFactory;
    ProcessingTimeService* timeService = nullptr;
    long periodicWatermarkInterval;

    ScheduledFutureTask* periodicEmitHandle = nullptr;

    void TriggerPeriodicEmit(long wallClockTimestamp);

    SplitLocalOutputs* currentPerSplitOutputs = nullptr;
    StreamingReaderOutput* currentMainOutput = nullptr;
    IdlenessManager* idlenessManager = nullptr;
};

#endif // OMNISTREAM_PROGRESSIVETIMESTAMPSANDWATERMARKS_H
