/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OUTPUT_FILE_CONFIG_H
#define OUTPUT_FILE_CONFIG_H

#include <string>

class OutputFileConfigBuilder;

class OutputFileConfig {
public:
    OutputFileConfig(const std::string &partPrefix, const std::string &partSuffix)
        : partPrefix(partPrefix),
          partSuffix(partSuffix) {}

    const std::string &getPartPrefix() const
    {
        return partPrefix;
    }

    const std::string &getPartSuffix() const
    {
        return partSuffix;
    }

    static inline OutputFileConfigBuilder builder();

private:
    const std::string partPrefix;
    const std::string partSuffix;
};

class OutputFileConfigBuilder {
public:
    inline OutputFileConfigBuilder();

    inline OutputFileConfigBuilder &withPartPrefix(const std::string &prefix)
    {
        this->partPrefix = prefix;
        return *this;
    }

    inline OutputFileConfigBuilder &withPartSuffix(const std::string &suffix)
    {
        this->partSuffix = suffix;
        return *this;
    }

    inline OutputFileConfig build()
    {
        return OutputFileConfig(partPrefix, partSuffix);
    }

private:
    static inline const std::string DEFAULT_PART_PREFIX = "part";
    static inline const std::string DEFAULT_PART_SUFFIX = "";
    std::string partPrefix;
    std::string partSuffix;
};

inline OutputFileConfigBuilder::OutputFileConfigBuilder()
    : partPrefix(DEFAULT_PART_PREFIX),
      partSuffix(DEFAULT_PART_SUFFIX) {}

inline OutputFileConfigBuilder OutputFileConfig::builder()
{
    return OutputFileConfigBuilder();
}

#endif // OUTPUT_FILE_CONFIG_H