/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#ifndef MULTIPLERECORDWRITERSV2_H
#define MULTIPLERECORDWRITERSV2_H
#include "RecordWriterDelegateV2.h"

namespace omnistream {
    class MultipleRecordWritersV2 : public RecordWriterDelegateV2 {
    public:
        explicit MultipleRecordWritersV2(std::vector<RecordWriterV2*>& recordWriters);

        RecordWriterV2* getRecordWriter(int outputIndex) override;

        ~MultipleRecordWritersV2() override = default;

        void close() override;

        void cancel() override;

        void broadcastEvent(std::shared_ptr<AbstractEvent> event) override
        {
            for (auto writer : recordWriters) {
                writer->broadcastEvent(event);
            }
        }

        std::shared_ptr<CompletableFuture> GetAvailableFuture() override
        {
            for (int i = 0; i < recordWriters.size(); i++) {
                futures.push_back(recordWriters[i]->GetAvailableFuture());
            }
            return CompletableFuture::allOf(futures);
        }

        bool isAvailable() override
        {
            for (auto recordWriter : recordWriters) {
                if (!recordWriter->isAvailable()) {
                    return false;
                }
            }
            return true;
        }

    private:
        std::vector<RecordWriterV2*> recordWriters;
        std::vector<std::shared_ptr<CompletableFuture>> futures;
    };
}

#endif
