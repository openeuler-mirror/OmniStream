/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef OMNIFLINK_KAFKAWRITER_H
#define OMNIFLINK_KAFKAWRITER_H

#include <deque>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>

#include <librdkafka/rdkafkacpp.h>
#include "KafkaCommittable.h"
#include "KafkaWriterState.h"
#include "TransactionAborter.h"
#include "KafkaRecordSerializationSchema.h"
#include "DynamicKafkaRecordSerializationSchema.h"
#include "TransactionalIdFactory.h"
#include "core/include/common.h"
#include "table/data/binary/BinaryRowData.h"
#include "DeliveryGuarantee.h"
#include "atomic"
#include "table/vectorbatch/VectorBatch.h"
#include "include/common.h"
#include "bind_core_manager.h"

class KafkaWriter {
public:
    KafkaWriter(DeliveryGuarantee deliveryGuarantee,
                RdKafka::Conf* kafkaProducerConfig,
                std::string& transactionalIdPrefix,
                std::string& topic,
                const nlohmann::json& description,
                int64_t maxPushRecords);
    KafkaWriter();
    ~KafkaWriter();

    void write(String *element);
    void write(Row *element);
    void write(RowData *element);

    void produce(RdKafka::Producer* kafkaProducer,
                 RdKafka::Topic *rd_topic,
                 const std::vector<char*>& value,
                 const std::vector<size_t>& valuesLen);

    void Flush(bool endOfInput);
    std::vector<KafkaCommittable> prepareCommit();
    std::vector<KafkaWriterState> recoveredStates;
    RdKafka::Conf* kafkaProducerConfig;
    std::string topic;

    void SetSubTaskIdx(int32_t subtaskIdx);

private:
    static constexpr const char* KAFKA_PRODUCER_METRIC_NAME = "KafkaProducer";
    static constexpr long METRIC_UPDATE_INTERVAL_MILLIS = 500;

    static constexpr const char* KEY_DISABLE_METRICS = "flink.disable-metrics";
    static constexpr const char* KEY_REGISTER_METRICS = "register.producer.metrics";
    static constexpr const char* KAFKA_PRODUCER_METRICS = "producer-metrics";

    DeliveryGuarantee deliveryGuarantee;
    std::string transactionalIdPrefix = "kafka-sink";
    DynamicKafkaRecordSerializationSchema *recordSerializer;
    std::exception_ptr asyncProducerException;
    nlohmann::json description;
    std::vector<std::string> inputFields;
    std::vector<std::string> inputTypes;

    long latestOutgoingByteTotal{};
    std::shared_ptr<FlinkKafkaInternalProducer> currentProducer1;
    std::shared_ptr<FlinkKafkaInternalProducer> currentProducer2;
    std::deque<std::shared_ptr<FlinkKafkaInternalProducer>> producerPool;
    long lastCheckpointId{};

    bool closed = false;
    long lastSync = 0;

    std::vector<char*> values;
    std::vector<size_t> valuesLens;
    int limit = 10000; // 10000
    int cur = 0;

    std::thread worker_thread;      // 后台工作线程
    std::mutex queueMutex;         // 任务队列互斥锁
    std::condition_variable cv;     // 条件变量（用于唤醒线程）
    std::queue<std::function<void()>> tasks; // 任务队列
    std::atomic<bool> stop_flag{false};      // 停止标志

    std::recursive_mutex gMtx;
    timespec start;
    timespec end;

    std::thread time_worker_thread;
    std::atomic<bool> timer_worker_thread_flag{true};
    std::vector<KeyValueByteContainer> records;

    void AbortCurrentProducer();
    void abortLingeringTransactions(const std::vector<KafkaWriterState>& recoveredStates, long startCheckpointId);
    std::shared_ptr<FlinkKafkaInternalProducer> getTransactionalProducer(long checkpointId);
    std::shared_ptr<FlinkKafkaInternalProducer> getOrCreateTransactionalProducer(const std::string& transactionalId);
    void ProduceRecord(KeyValueByteContainer &record);
    void handleRecord();
    RdKafka::Topic *rd_topic1;
    RdKafka::Topic *rd_topic2;
    int32_t partitionNum = 0;
    int32_t instanceId = 0;
    int producerIndexOne = 1;
    int producerIndexTwo = 2;
    int32_t bindCore = -1;
    bool binded = false;

    void  Init()
    {
        instanceId = getInstanceId();
        values.reserve(limit);
        valuesLens.reserve(limit);
        records.reserve(limit);
    };

    void timer_thread()
    {
        const int SLEEP_TIME = 5;
        while (timer_worker_thread_flag.load()) {
            clock_gettime(CLOCK_MONOTONIC, &end);
            // 计算时间差
            long ns = (end.tv_sec - start.tv_sec) * 1000000000L + (end.tv_nsec - start.tv_nsec);
            if (ns >= 5000000000L) {
                handleRecord();
                start = end;
            }

            // 降低CPU占用，每秒检查一次
            std::this_thread::sleep_for(std::chrono::seconds(SLEEP_TIME));
        }
    }

    void WorkerThreadFunc()
    {
        if (bindCore >= 0) {
            omnistream::BindCoreManager::GetInstance()->BindDirectCore(bindCore);
        }
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(queueMutex);
                // 等待任务或停止信号
                cv.wait(lock, [this]() {
                    return !tasks.empty() || stop_flag.load();
                });

                // 如果收到停止信号且队列为空，退出线程
                if (stop_flag.load() && tasks.empty()) {
                    return;
                }

                // 取出任务
                task = std::move(tasks.front());
                tasks.pop();
            }

            // 执行任务
            task();
        }
    }

    static int32_t getInstanceId()
    {
        static std::atomic<int32_t > instanceIdAtomic = 0;
        auto retId = instanceIdAtomic.fetch_add(1, std::memory_order_seq_cst);
        return retId;
    }
};

#endif // OMNIFLINK_KAFKAWRITER_H
