/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_CONFIGURATION_H
#define FLINK_TNEL_CONFIGURATION_H
#include "basictypes/Object.h"
#include "basictypes/Integer.h"
#include "basictypes/java_util_HashMap.h"

class Configuration : public Object {
public:
    Configuration();

    explicit Configuration(bool standardYaml);

    Configuration(const Configuration &other);

    void setString(std::string key, String* value);

    void setString(std::string key, std::string value);

    void setString(String* key, std::string value);

    void setString(String* key, String * value);

    void setInteger(std::string key, int32_t value);

    void setBoolean(std::string key, bool value);

    bool containsKey(std::string key);

    String* getString(std::string key, std::string defaultValue);

    String* getString(String * key, std::string defaultValue);

    String* getString(std::string key, String * defaultValue);

    String* getString(String * key, String * defaultValue);

    int32_t getInteger(std::string key, int32_t defaultValue);

    bool getBoolean(std::string key, bool defaultValue);

    HashMap *getMap();

private:
    bool standardYaml = true;
    HashMap confData;
};

#endif // FLINK_TNEL_CONFIGURATION_H
