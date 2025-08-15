/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "functions/Configuration.h"

Configuration::Configuration() {
}

Configuration::Configuration(bool standardYaml): standardYaml(standardYaml) {
}

Configuration::Configuration(const Configuration &other) {
    standardYaml = other.standardYaml;
}

void Configuration::setString(std::string key, String * value) {
    // todo implement Configuration::setString and add libInterfaceRefs
}

void Configuration::setString(std::string key, std::string value) {
    // todo implement Configuration::setString and add libInterfaceRefs
}

void Configuration::setString(String * key, std::string value) {
    // todo implement Configuration::setString and add libInterfaceRefs
}

void Configuration::setString(String * key, String * value) {
    // todo implement Configuration::setString and add libInterfaceRefs
}

void Configuration::setInteger(std::string key, int32_t value) {
    // todo implement Configuration::setInteger and add libInterfaceRefs
}

void Configuration::setBoolean(std::string key, bool value) {
    // todo implement Configuration::setBoolean and add libInterfaceRefs
}

bool Configuration::containsKey(std::string key) {
    // todo implement Configuration::containsKey and add libInterfaceRefs
    return {};
}

String* Configuration::getString(std::string key, std::string defaultValue) {
    // todo implement Configuration::getString and add libInterfaceRefs
    return {};
}

String* Configuration::getString(String * key, std::string defaultValue) {
    // todo implement Configuration::getString and add libInterfaceRefs
    return {};
}

String* Configuration::getString(std::string key, String * defaultValue) {
    // todo implement Configuration::getString and add libInterfaceRefs
    return {};
}

String* Configuration::getString(String * key, String * defaultValue) {
    // todo implement Configuration::getString and add libInterfaceRefs
    return {};
}

int32_t Configuration::getInteger(std::string key, int32_t defaultValue) {
    // todo implement Configuration::getInteger and add libInterfaceRefs
    return {};
}

bool Configuration::getBoolean(std::string key, bool defaultValue) {
    // todo implement Configuration::getBoolean and add libInterfaceRefs
    return {};
}

HashMap* Configuration::getMap() {
    return &confData;
}