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

#include <stdexcept>
#include "StreamRecord.h"
#include "basictypes/Tuple2.h"
#include "StreamElementSerializer.h"

namespace omnistream::datastream {
    StreamElementSerializer::StreamElementSerializer(TypeSerializer *typeSerializer)
        : typeSerializer_(typeSerializer), reUsableRecord_(nullptr), reUsableWatermark_(nullptr)
    {
        reUsableRecord_ = new StreamRecord();
        reUsableWatermark_ = new Watermark(0);
    }

    void *StreamElementSerializer::deserialize(DataInputView &source)
    {
        int tag = static_cast<uint8_t>(source.readByte());
#ifdef DEBUG
        LOG("tag: " + std::to_string(tag))
#endif
        if (tag == static_cast<int>(StreamElementTag::TAG_REC_WITH_TIMESTAMP)) {
            long timestamp = source.readLong();
#ifdef DEBUG
            LOG("timestamp: " + std::to_string(timestamp))
            LOG("typeSerializer_: is kind of  " << typeSerializer_->getName());
#endif
            // Check the typeSerializer_
            if (strcmp(typeSerializer_->getName(), "BinaryRowDataSerializer") == 0) {
                auto binaryRowData = typeSerializer_->deserialize(source);
                reUsableRecord_->setValue(binaryRowData);
                reUsableRecord_->setTag(StreamElementTag::TAG_REC_WITH_TIMESTAMP);
                reUsableRecord_->setTimestamp(timestamp);
            } else {
                Object* buffer = typeSerializer_->GetBuffer();
                typeSerializer_->deserialize(buffer, source);
                reUsableRecord_->setValue(buffer);

                reUsableRecord_->setTag(StreamElementTag::TAG_REC_WITH_TIMESTAMP);
                reUsableRecord_->setTimestamp(timestamp);
            }

            return reUsableRecord_;
        } else if (tag == static_cast<int>(StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP)) {
#ifdef DEBUG
            LOG("typeSerializer_: is kind of  " << typeSerializer_->getName());
#endif
            if (strcmp(typeSerializer_->getName(), "BinaryRowDataSerializer") == 0) {
                auto binaryRowData = typeSerializer_->deserialize(source);
                reUsableRecord_->setValue(binaryRowData);
            } else {
                Object* buffer = typeSerializer_->GetBuffer();
                typeSerializer_->deserialize(buffer, source);
                reUsableRecord_->setValue(buffer);
            }
            return reUsableRecord_;
        } else if (tag == static_cast<int>(StreamElementTag::TAG_WATERMARK)) {
            long timestamp = source.readLong();
            reUsableWatermark_->setTimestamp(timestamp);
            reUsableWatermark_->setTag(StreamElementTag::TAG_WATERMARK);
            return reUsableWatermark_;
        } else if (tag == static_cast<int>(StreamElementTag::TAG_STREAM_STATUS)) {
            return new Watermark(source.readInt());
        } else {
            THROW_LOGIC_EXCEPTION("Corrupt stream, found tag:" +  std::to_string(tag));
        }
    }

    void StreamElementSerializer::serialize(Object* input, DataOutputSerializer &target)
    {
        auto element = reinterpret_cast<StreamElement *>(input);
#ifdef DEBUG
        LOG(">>>> Tag: " + std::to_string(static_cast<uint8_t > (element->getTag())))
#endif
        if (element->getTag() == StreamElementTag::TAG_REC_WITH_TIMESTAMP ||
            element->getTag() == StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP) {
            auto* asRecord = static_cast<StreamRecord *>(element);

            if (asRecord->hasTimestamp()) {
                target.write(static_cast<uint32_t>(StreamElementTag::TAG_REC_WITH_TIMESTAMP));
                target.writeRecordTimestamp(asRecord->getTimestamp());
            } else {
                target.write(static_cast<uint32_t>(StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP));
            }
#ifdef DEBUG
            LOG(">>> After write tag:" << typeSerializer_->getName())
#endif
            typeSerializer_->serialize(static_cast<Object *>(asRecord->getValue()), target);
        } else if (element->getTag() == StreamElementTag::TAG_WATERMARK) {
            target.write(static_cast<uint8_t>(StreamElementTag::TAG_WATERMARK));
            auto* asWatermark = static_cast<Watermark *>(element);
            target.writeLong(asWatermark->getTimestamp());
        } else {
            std::cout << "type name: " << typeSerializer_->getName() << std::endl;
            THROW_LOGIC_EXCEPTION("Unknown element_, can not serialize it" + std::to_string(static_cast<uint8_t >(element->getTag())));
        }
    }

    const char *StreamElementSerializer::getName() const
    {
        return "StreamElementSerializer";
    }

}

