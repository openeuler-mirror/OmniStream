/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_CONFIGLOADER_H
#define FLINK_TNEL_CONFIGLOADER_H

#include <unordered_map>
#include <string>

class ConfigLoader {
public:
    static std::unordered_map<std::string, std::string> LoadKafkaConfig(const std::string &confFile);

    static std::unordered_map<std::string, std::string> LoadKafkaConsumerConfig();

    static std::unordered_map<std::string, std::string> LoadKafkaProducerConfig();

    static std::unordered_map<std::string, std::string>& GetKafkaConsumerConfig();

    static std::unordered_map<std::string, std::string>& GetKafkaProducerConfig();

    static std::unordered_map<std::string, std::string> g_consumer_config;

    static std::unordered_map<std::string, std::string> g_producer_config;
};

#endif // FLINK_TNEL_CONFIGLOADER_H
