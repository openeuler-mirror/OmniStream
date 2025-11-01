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
#ifndef FLINK_TNEL_CONFIGURATION_H
#define FLINK_TNEL_CONFIGURATION_H
#include "basictypes/Object.h"
#include "basictypes/Integer.h"
#include "basictypes/java_util_HashMap.h"
#include "core/configuration/ConfigOption.h"

class Configuration : public Object {
public:
    Configuration();

    ~Configuration();

    explicit Configuration(bool standardYaml);

    Configuration(const Configuration &other);

    void setString(std::string key, String* value);

    void setString(std::string key, std::string value);

    void setString(String* key, std::string value);

    void setString(String* key, String* value);

    void setInteger(std::string key, int32_t value);

    void setBoolean(std::string key, bool value);

    bool containsKey(std::string key);

    String* getString(std::string key, std::string defaultValue);

    String* getString(String* key, std::string defaultValue);

    String* getString(std::string key, String* defaultValue);

    String* getString(String* key, String* defaultValue);

    int32_t getInteger(std::string key, int32_t defaultValue);

    bool getBoolean(std::string key, bool defaultValue);

    HashMap *getMap();

    static Configuration* TM_CONFIG;

    Object* getValue(ConfigOption* configOption);

    Object* getValue(const std::unique_ptr<ConfigOption> &configOption);

    Object* returnDefaultValue(ConfigOption* configOption);

    void setConfiguration(nlohmann::json config);

private:
    bool standardYaml = true;
    HashMap* confData = nullptr;
};

#endif // FLINK_TNEL_CONFIGURATION_H
