/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "KafkaWriter.h"
#include <chrono>
#include <cstddef>
#include "bind_core_manager.h"
#include "include/common.h"

KafkaWriter::KafkaWriter(DeliveryGuarantee deliveryGuarantee,
                         RdKafka::Conf *kafkaProducerConfig,
                         std::string &transactionalIdPrefix,
                         std::string &topic,
                         const nlohmann::json& description,
                         int64_t maxPushRecords)
    : kafkaProducerConfig(kafkaProducerConfig),
    topic(topic),
    deliveryGuarantee(deliveryGuarantee),
    transactionalIdPrefix(transactionalIdPrefix),
    description(description),
    limit(maxPushRecords) {
    Init();
    if (description["batch"]) {
        inputFields = description["inputFields"].get<std::vector<std::basic_string<char>>>();
        inputTypes = description["inputTypes"].get<std::vector<std::basic_string<char>>>();
        recordSerializer = new DynamicKafkaRecordSerializationSchema(inputFields, inputTypes);
    }

    currentProducer1 =
            std::make_shared<FlinkKafkaInternalProducer>(kafkaProducerConfig, std::to_string(producerIndexOne));
    currentProducer2 =
            std::make_shared<FlinkKafkaInternalProducer>(kafkaProducerConfig, std::to_string(producerIndexTwo));
    RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    std::string errstr;
    this->kafkaProducerConfig->set("default_topic_conf", tconf, errstr);
    rd_topic1 = RdKafka::Topic::create(currentProducer1->getKafkaProducer(), topic, tconf, errstr);
    rd_topic2 = RdKafka::Topic::create(currentProducer2->getKafkaProducer(), topic, tconf, errstr);
    partitionNum = rd_topic1->get_partition_num();

    time_worker_thread = std::thread(&KafkaWriter::timer_thread, this);
}

KafkaWriter::KafkaWriter()
{
    Init();
    kafkaProducerConfig = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    std::string errstr;
    kafkaProducerConfig->set("bootstrap.servers", "127.0.0.1:9092", errstr);
    kafkaProducerConfig->set("transaction.timeout.ms", "600000", errstr);
    currentProducer1 = getTransactionalProducer(lastCheckpointId + 1);
}

KafkaWriter::~KafkaWriter()
{
    delete kafkaProducerConfig;
    delete rd_topic1;
    delete rd_topic2;
    delete recordSerializer;
    stop_flag.store(true);
    cv.notify_all();

    // 等待线程结束
    if (worker_thread.joinable()) {
        worker_thread.join();
    }

    timer_worker_thread_flag.store(false);
    if (time_worker_thread.joinable()) {
        time_worker_thread.join();
    }
}

void KafkaWriter::write(String *element)
{
    if (unlikely(not binded)) {
        if (bindCore >= 0) {
            omnistream::BindCoreManager::GetInstance()->BindDirectCore(bindCore);
        }
        binded = true;
    }
    auto record = recordSerializer->Serialize(element);
    ProduceRecord(record);
}

void KafkaWriter::write(Row *element)
{
    auto record = recordSerializer->Serialize(element);
    ProduceRecord(record);
}

void KafkaWriter::write(RowData *element)
{
    auto record = recordSerializer->Serialize(element);
    ProduceRecord(record);
}

void KafkaWriter::Flush(bool endOfInput)
{
    if (deliveryGuarantee != DeliveryGuarantee::NONE || endOfInput) {
        handleRecord();
        currentProducer1->Flush();
        currentProducer2->Flush();
    }
}

std::vector<KafkaCommittable> KafkaWriter::prepareCommit()
{
    if (deliveryGuarantee == DeliveryGuarantee::EXACTLY_ONCE) {
        auto committables = std::vector<KafkaCommittable>{
            KafkaCommittable::of(currentProducer1.get(), [this](FlinkKafkaInternalProducer *producer) {
                producerPool.push_back(static_cast<const std::shared_ptr<FlinkKafkaInternalProducer>>(producer));
            })};
        return committables;
    }
    return {};
}

void KafkaWriter::AbortCurrentProducer()
{
    if (currentProducer1->IsInTransaction()) {
        currentProducer1->AbortTransaction();
    }
}

void KafkaWriter::abortLingeringTransactions(const std::vector<KafkaWriterState> &recoveredStates,
                                             long startCheckpointId)
{
    auto prefixesToAbort = std::vector<std::string>{transactionalIdPrefix};

    if (!recoveredStates.empty()) {
        const auto &lastState = recoveredStates.front();
        if (lastState.getTransactionalIdPrefix() != transactionalIdPrefix) {
            prefixesToAbort.push_back(lastState.getTransactionalIdPrefix());
        }
    }

    TransactionAborter transactionAborter(
            [this](const std::string &transactionalId) { return getOrCreateTransactionalProducer(transactionalId); },
            [this](const std::shared_ptr<FlinkKafkaInternalProducer> &producer) { producerPool.push_back(producer); });
    transactionAborter.abortLingeringTransactions(prefixesToAbort, startCheckpointId);
}

std::shared_ptr<FlinkKafkaInternalProducer> KafkaWriter::getTransactionalProducer(long checkpointId)
{
    std::shared_ptr<FlinkKafkaInternalProducer> producer;
    for (long id = lastCheckpointId + 1; id <= checkpointId; id++) {
        auto transactionalId = TransactionalIdFactory::buildTransactionalId(transactionalIdPrefix, 0, id);
        producer = getOrCreateTransactionalProducer(transactionalId);
    }
    lastCheckpointId = checkpointId;
    return producer;
}

std::shared_ptr<FlinkKafkaInternalProducer> KafkaWriter::getOrCreateTransactionalProducer(
    const std::string &transactionalId)
{
    auto producer = producerPool.empty() ? nullptr : producerPool.front();
    if (!producer) {
        producer = std::make_shared<FlinkKafkaInternalProducer>(kafkaProducerConfig, transactionalId);
    } else {
        producerPool.pop_front();
    }
    return producer;
}

void KafkaWriter::ProduceRecord(KeyValueByteContainer &record)
{
    std::unique_lock<std::recursive_mutex> gLock(gMtx);
    values.push_back(record.value);
    valuesLens.push_back(record.valueLen);
    records.push_back(std::move(record));
    ++cur;
    if (cur >= limit) {
        handleRecord();
        clock_gettime(CLOCK_MONOTONIC, &start);
    }
}

void KafkaWriter::handleRecord()
{
    std::unique_lock<std::recursive_mutex> gLock(gMtx);
    const size_t mid = cur / 2;
    // 分割数据
    std::vector<char*> first_half(values.begin(), values.begin() + mid);
    std::vector<size_t> first_half_lens(valuesLens.begin(), valuesLens.begin() + mid);
    std::vector<char*> last_half(values.begin() + mid, values.end());
    std::vector<size_t> last_half_lens(valuesLens.begin() + mid, valuesLens.end());

    auto kafkaProducer1 = currentProducer1->getKafkaProducer();

    produce(kafkaProducer1, rd_topic1, first_half, first_half_lens);

    {
        std::lock_guard<std::mutex> lock(queueMutex);
        tasks.emplace([this, last_half, last_half_lens]() {
            auto kafkaProducer2 = currentProducer2->getKafkaProducer();
            produce(kafkaProducer2, rd_topic2, last_half, last_half_lens);
        });
    }
    cv.notify_one();

    values.clear();
    valuesLens.clear();
    records.clear();
    cur = 0;
}

void KafkaWriter::SetSubTaskIdx(int32_t subtaskIdx)
{
    this->instanceId = subtaskIdx;
    if (omnistream::BindCoreManager::GetInstance()->NeedBindSink()) {
        bindCore = omnistream::BindCoreManager::GetInstance()->GetSinkCore(subtaskIdx);
    }
    worker_thread = std::thread(&KafkaWriter::WorkerThreadFunc, this);
}

void KafkaWriter::produce(RdKafka::Producer* kafkaProducer,
                          RdKafka::Topic *rd_topic,
                          const std::vector<char*>& value,
                          const std::vector<size_t>& valuesLen)
{
    partitionNum = rd_topic->get_partition_num();
    int32_t realPartition = partitionNum == 0 ? RdKafka::Topic::PARTITION_UA : (instanceId % partitionNum);
    RdKafka::ErrorCode resp = kafkaProducer->produce(rd_topic,
                                                     realPartition,
                                                     RdKafka::Producer::RK_MSG_FREE |  RdKafka::Producer::RK_MSG_BLOCK,
                                                     value,
                                                     valuesLen,
                                                     nullptr,
                                                     0,
                                                     nullptr);
    if (resp != RdKafka::ERR_NO_ERROR) {
        LOG("Produce failed:" << RdKafka::err2str(resp))
    }

    kafkaProducer->poll(0);
}