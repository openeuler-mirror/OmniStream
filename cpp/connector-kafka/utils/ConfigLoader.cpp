/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "ConfigLoader.h"
#include <fstream>

static void Trim(std::string &value)
{
    value.erase(0, value.find_first_not_of(' '));
    value.erase(value.find_last_not_of(' ') + 1);
}

static std::string GetOmniHome()
{
    auto omniHome = std::getenv("OMNI_HOME");
    if (omniHome != nullptr && omniHome[0] != '\0') {
        std::string confDir { omniHome };
        Trim(confDir);
        return confDir;
    } else {
        return "/opt";
    }
}

static std::string GetConfigFilePath(const std::string &confFile)
{
    return GetOmniHome() + confFile;
}

std::unordered_map<std::string, std::string> ConfigLoader::LoadKafkaConfig(const std::string &configFilePath)
{
    std::unordered_map<std::string, std::string> kafkaConfig;
    auto configFileRealPath = realpath(configFilePath.c_str(), nullptr);
    if (configFileRealPath == nullptr) {
        return kafkaConfig;
    }

    std::ifstream confInput(configFileRealPath);
    if (!confInput.is_open()) {
        free(configFileRealPath);
        return kafkaConfig;
    }

    std::string s;
    while (std::getline(confInput, s)) {
        Trim(s);
        if (s.empty() || s[0] == '#') {
            continue;
        }
        auto pos = s.find('=');
        if (pos == std::string::npos) {
            continue;
        }
        auto key = s.substr(0, pos);
        auto value = s.substr(pos + 1);
        Trim(key);
        Trim(value);

        kafkaConfig[key] = value;
    }
    confInput.close();
    free(configFileRealPath);
    return kafkaConfig;
}

std::unordered_map<std::string, std::string> ConfigLoader::LoadKafkaConsumerConfig()
{
    std::string confFile = "/conf/kafka_consumer.conf";
    auto configFilePath = GetConfigFilePath(confFile);
    return LoadKafkaConfig(configFilePath);
}

std::unordered_map<std::string, std::string> ConfigLoader::LoadKafkaProducerConfig()
{
    std::string confFile = "/conf/kafka_producer.conf";
    auto configFilePath = GetConfigFilePath(confFile);
    return LoadKafkaConfig(configFilePath);
}

std::unordered_map<std::string, std::string> ConfigLoader::g_consumer_config = ConfigLoader::LoadKafkaConsumerConfig();

std::unordered_map<std::string, std::string> ConfigLoader::g_producer_config = ConfigLoader::LoadKafkaProducerConfig();

std::unordered_map<std::string, std::string>& ConfigLoader::GetKafkaConsumerConfig()
{
    return g_consumer_config;
}

std::unordered_map<std::string, std::string>& ConfigLoader::GetKafkaProducerConfig()
{
    return g_producer_config;
}