/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: Spilling Adaptive Spanning Record Deserializer for DataStream
 */
#include <iostream>
#include "../include/common.h"
#include "SpillingAdaptiveSpanningRecordDeserializer.h"
namespace omnistream::datastream {
    SpillingAdaptiveSpanningRecordDeserializer::SpillingAdaptiveSpanningRecordDeserializer()
    {
        this->nonSpanningWrapper = new NonSpanningWrapper();
        this->spanningWrapper = new SpanningWrapper();
    }

    DeserializationResult &SpillingAdaptiveSpanningRecordDeserializer::getNextRecord(IOReadableWritable &target)
    {
        // always check the non-spanning wrapper first.
        // this should be the majority of the cases for small records
        // for large records, this portion of the work is very small in comparison anyways

        DeserializationResult &result = readNextRecord(target);
        return result;
    }

    DeserializationResult &SpillingAdaptiveSpanningRecordDeserializer::readNextRecord(IOReadableWritable &target)
    {
        if (nonSpanningWrapper->hasCompleteLength()) {
            LOG(" Inside SpillingAdaptiveSpanningRecordDeserializer::readNextRecord  nonSpanningWrapper->hasCompleteLength()")
            return readNonSpanningRecord(target);
        } else if (nonSpanningWrapper->hasRemaining()) {
            LOG(" Inside SpillingAdaptiveSpanningRecordDeserializer::readNextRecord  nonSpanningWrapper->hasRemaining()")

            nonSpanningWrapper->transferTo(*(spanningWrapper->lengthBuffer_));
            return DeserializationResult_PARTIAL_RECORD;
        } else if (spanningWrapper->hasFullRecord()) {
            LOG(" Inside SpillingAdaptiveSpanningRecordDeserializer::readNextRecord  spanningWrapper->hasFullRecord()")

            target.read(spanningWrapper->getInputView());
            spanningWrapper->transferLeftOverTo(*nonSpanningWrapper);
            return nonSpanningWrapper->hasRemaining()
                   ? DeserializationResult_INTERMEDIATE_RECORD_FROM_BUFFER
                   : DeserializationResult_LAST_RECORD_FROM_BUFFER;
        }
        return DeserializationResult_PARTIAL_RECORD;
        ;
    }

    DeserializationResult &SpillingAdaptiveSpanningRecordDeserializer::readNonSpanningRecord(IOReadableWritable &target)
    {
        LOG(">>>>>>>>>>")

        int recordLen = nonSpanningWrapper->readInt();
#ifdef DEBUG
        LOG(" After nonSpanningWrapper->readInt (recordLen): " + std::to_string(recordLen));
#endif
        if (nonSpanningWrapper->canReadRecord(recordLen)) {
            return nonSpanningWrapper->readInto(target);
        } else {
            // LOG(" spanningWrapper->transferFrom: with the recordLen is" << recordLen);
            spanningWrapper->transferFrom(*nonSpanningWrapper, recordLen);
            return DeserializationResult_PARTIAL_RECORD;
        }
    }

    void SpillingAdaptiveSpanningRecordDeserializer::setNextBuffer(const uint8_t *buffer, int size)
    {
        LOG(">>>>> SpillingAdaptiveSpanningRecordDeserializer::setNextBuffer Begin")
        // need more investigation on buffer_ owner ship. now assume buffer_ is still owned by caller
        if (spanningWrapper->getNumGatheredBytes() > 0) {
#ifdef DEBUG
            LOG("   spanningWrapper->addNextChunkFromMemoryBuffer buffer " +
                std::to_string(reinterpret_cast<long> (buffer)) + " size " + std::to_string(size))
#endif
            spanningWrapper->addNextChunkFromMemoryBuffer(buffer, size); // copy the buffer_ data into spanning wrapper
        } else {
#ifdef DEBUG
            LOG("   nonSpanningWrapper->initializeFromMemoryBuffer " +
                std::to_string(reinterpret_cast<long> (buffer)) + " size " + std::to_string(size))
#endif
            nonSpanningWrapper->initializeFromMemoryBuffer(buffer, size); // zero copy, a view of buffer_ data
        }
        LOG(">>>>> SpillingAdaptiveSpanningRecordDeserializer::setNextBuffer End")
    }

    void SpillingAdaptiveSpanningRecordDeserializer::clear()
    {
        nonSpanningWrapper->clear();
        spanningWrapper->clear();
    }

    void *SpillingAdaptiveSpanningRecordDeserializer::deserialize(DataInputView &source)
    {
        NOT_IMPL_EXCEPTION;
    }

    void SpillingAdaptiveSpanningRecordDeserializer::serialize(void *record, DataOutputSerializer &target)
    {
        NOT_IMPL_EXCEPTION;
    }

    const char *SpillingAdaptiveSpanningRecordDeserializer::getName() const
    {
        return "SpillingAdaptiveSpanningRecordDeserializer";
    }
}