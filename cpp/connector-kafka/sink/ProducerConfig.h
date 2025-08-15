/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_PRODUCERCONFIG_H
#define FLINK_TNEL_PRODUCERCONFIG_H

#include <iostream>
#include <map>

static std::map<std::string, std::string> ProducerConfig {
        {"fetch.max.wait.ms", "fetch.wait.max.ms"},
        {"key.deserializer", ""},
        {"value.deserializer", ""},
        {"bootstrap.servers", "bootstrap.servers"},
        {"client.dns.lookup", "client.dns.lookup"},
        {"metadata.max.age.ms", "metadata.max.age.ms"},
        {"send.buffer.bytes", "socket.send.buffer.bytes"},
        {"receive.buffer.bytes", "socket.receive.buffer.bytes"},
        {"client.id", "client.id"},
        {"client.rack", "client.rack"},
        {"reconnect.backoff.ms", "reconnect.backoff.ms"},
        {"reconnect.backoff.max.ms", "reconnect.backoff.max.ms"},
        {"retry.backoff.ms", "retry.backoff.ms"},
        {"socket.connection.setup.timeout.ms", "socket.connection.setup.timeout.ms"},
        {"connections.max.idle.ms", "connections.max.idle.ms"},
        {"allow.auto.create.topics", "allow.auto.create.topics"},
        {"security.protocol", "security.protocol"},
        {"sasl.mechanism", "sasl.mechanism"},
        {"metadata.max.idle.ms", ""},
        {"batch.size", "batch.size"},
        {"acks", "acks"},
        {"linger.ms", "linger.ms"},
        {"request.timeout.ms", "request.timeout.ms"},
        {"delivery.timeout.ms", "delivery.timeout.ms"},
        {"max.request.size", ""},
        {"max.block.ms", "socket.blocking.max.ms"},
        {"buffer.memory", ""},
        {"compression.type", "compression.type"},
        {"metrics.sample.window.ms", ""},
        {"metrics.num.samples", ""},
        {"metrics.recording.level", ""},
        {"metric.reporters", ""},
        {"max.in.flight.requests.per.connection", ""},
        {"retries", "retries"},
        {"partitioner.class", ""}, // This needs attention.
        {"interceptor.classes", ""},
        {"enable.idempotence", "enable.idempotence"},
        {"transaction.timeout.ms", "transaction.timeout.ms"},
        {"transactional.id", "transactional.id"},
        {"security.providers", ""}
};

#endif // FLINK_TNEL_PRODUCERCONFIG_H
