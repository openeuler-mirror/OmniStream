#ifndef FLINK_TNEL_MOCKS_H
#define FLINK_TNEL_MOCKS_H

#include "core/operators/StreamMap.h"
#include "functions/MapFunction.h"
#include "streamrecord/StreamRecord.h"
#include "task/WatermarkGaugeExposingOutput.h"
#include <librdkafka/rdkafkacpp.h>

class MockObject : public Object {
public:
    MockObject(int value) : value(value) {}
    int getValue() { return value; }
private:
    int value;
};

class MockStringObject : public Object {
public:
    explicit MockStringObject(const std::string& value) : value(value) {}

    std::string toString() override {
        return value;
    }

    Object* clone() override {
        return new MockStringObject(value);
    }

    int hashCode() override {
        int hash = (int)std::hash<std::string>{}(value);
        return hash;
    }

    bool equals(Object *obj) override {
        MockStringObject* other = dynamic_cast<MockStringObject*>(obj);
        return other && value == other->value;
    }

private:
    std::string value;
};

class MockLongObject : public Object {
public:
    MockLongObject(int64_t value) : value(value) {}

    std::string toString() override {
        return std::to_string(value);
    }

    Object* clone() override {
        return new MockLongObject(value);
    }

    int hashCode() override {
        return static_cast<int>(value ^ (static_cast<uint64_t>(value) >> 32));
    }

    bool equals(Object *obj) override {
        MockLongObject* other = dynamic_cast<MockLongObject*>(obj);
        return other && value == other->value;
    }

private:
    int64_t value;
};

class MockOutput : public WatermarkGaugeExposingOutput {
public:
    void collect(void * record) override {
        StreamRecord *streamRecord = reinterpret_cast<StreamRecord *>(record);
        collectedRecords.push_back(streamRecord);
    }

    std::vector<StreamRecord*> getCollectedRecords() {
        return collectedRecords;
    }
    void close() override {}
    void emitWatermark(Watermark *watermark) override {
    }
    void emitWatermarkStatus(WatermarkStatus *watermarkStatus) override {}
private:
    std::vector<StreamRecord*> collectedRecords;
};

class MockStreamTaskStateInitializerImpl : public StreamTaskStateInitializerImpl {
    // 实现必要的方法
};

class MockTypeSerializer : public TypeSerializer {
    // 实现必要的方法
};

#endif //FLINK_TNEL_MOCKS_H
