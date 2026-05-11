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

#ifndef PARTITION_COMMIT_POLICY_H
#define PARTITION_COMMIT_POLICY_H

#include <string>
#include <vector>
#include <map>
#include <memory>
#include "FileSystemCommitter.h"

/**
 * Context passed to partition commit policies.
 */
class PolicyContext {
public:
    PolicyContext(const std::string &catalogName,
                  const std::string &databaseName,
                  const std::string &tableName,
                  const std::vector<std::string> &partitionKeys,
                  const std::vector<std::string> &partitionValues,
                  const std::string &partitionPath)
        : catalogName_(catalogName),
          databaseName_(databaseName),
          tableName_(tableName),
          partitionKeys_(partitionKeys),
          partitionValues_(partitionValues),
          partitionPath_(partitionPath) {}

    const std::string &catalogName() const { return catalogName_; }
    const std::string &databaseName() const { return databaseName_; }
    const std::string &tableName() const { return tableName_; }
    const std::vector<std::string> &partitionKeys() const { return partitionKeys_; }
    const std::vector<std::string> &partitionValues() const { return partitionValues_; }
    const std::string &partitionPath() const { return partitionPath_; }

    std::map<std::string, std::string> partitionSpec() const
    {
        std::map<std::string, std::string> spec;
        for (size_t i = 0; i < partitionKeys_.size() && i < partitionValues_.size(); ++i) {
            spec[partitionKeys_[i]] = partitionValues_[i];
        }
        return spec;
    }

private:
    std::string catalogName_;
    std::string databaseName_;
    std::string tableName_;
    std::vector<std::string> partitionKeys_;
    std::vector<std::string> partitionValues_;
    std::string partitionPath_;
};

/**
 * Interface for a partition commit policy.
 * Implementations must be idempotent - the same partition may be committed multiple times.
 */
class PartitionCommitPolicy {
public:
    virtual ~PartitionCommitPolicy() = default;

    virtual void commit(const PolicyContext &context) = 0;

    static constexpr const char *METASTORE = "metastore";
    static constexpr const char *SUCCESS_FILE = "success-file";
    static constexpr const char *CUSTOM = "custom";
};

/**
 * Policy that creates a _SUCCESS file in the partition directory to mark it as committed.
 */
class SuccessFileCommitPolicy : public PartitionCommitPolicy {
public:
    void commit(const PolicyContext &context) override
    {
        FileSystemCommitter::createSuccessFile(context.partitionPath());
        LOG("Committed partition with success file: " << context.partitionPath())
    }
};

/**
 * Policy that notifies a metastore about the committed partition.
 * In a filesystem-only setup, this is a no-op.
 */
class MetastoreCommitPolicy : public PartitionCommitPolicy {
public:
    void commit(const PolicyContext &context) override
    {
        LOG("MetastoreCommitPolicy: partition committed to metastore, path="
            << context.partitionPath())
    }
};

/**
 * Creates a chain of commit policies from the policy kind string.
 * Supported kinds (comma-separated): "success-file", "metastore", "custom"
 */
static std::vector<std::unique_ptr<PartitionCommitPolicy>> createPolicyChain(const std::string &policyKind)
{
    std::vector<std::unique_ptr<PartitionCommitPolicy>> policies;

    if (policyKind.empty()) {
        policies.push_back(std::make_unique<SuccessFileCommitPolicy>());
        return policies;
    }

    std::string kinds = policyKind;
    size_t pos = 0;
    while ((pos = kinds.find(',')) != std::string::npos) {
        std::string kind = kinds.substr(0, pos);
        addPolicy(policies, kind);
        kinds.erase(0, pos + 1);
    }
    addPolicy(policies, kinds);

    if (policies.empty()) {
        policies.push_back(std::make_unique<SuccessFileCommitPolicy>());
    }
    return policies;
}

static void addPolicy(std::vector<std::unique_ptr<PartitionCommitPolicy>> &policies,
                       const std::string &kind)
{
    std::string trimmed = kind;
    trimmed.erase(0, trimmed.find_first_not_of(" \t"));
    trimmed.erase(trimmed.find_last_not_of(" \t") + 1);

    if (trimmed == "success-file") {
        policies.push_back(std::make_unique<SuccessFileCommitPolicy>());
    } else if (trimmed == "metastore") {
        policies.push_back(std::make_unique<MetastoreCommitPolicy>());
    }
}

#endif // PARTITION_COMMIT_POLICY_H
