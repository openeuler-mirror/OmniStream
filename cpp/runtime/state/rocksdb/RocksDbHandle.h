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
#ifndef OMNISTREAM_ROCKSDBHANDLE_H
#define OMNISTREAM_ROCKSDBHANDLE_H

#pragma once

#include <stdexcept>
#include <memory>
#include <string>
#include <vector>
#include <filesystem>
#include <iostream>
#include <fstream>

#include "runtime/state/RegisteredStateMetaInfoBase.h"
#include "runtime/state/rocksdb/RocksDbOperationUtils.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/RocksDbKvStateInfo.h"
#include "streaming/runtime/metrics/MetricGroup.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "state/RocksDBMemoryOptions.h"

namespace fs = std::filesystem;

class  RocksDbHandle {
public:
    RocksDbHandle(std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>> *kvStateInformation,
                  const fs::path& instanceRocksDBPath,
                  std::shared_ptr<rocksdb::DBOptions> dbOptions,
                  std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory)
        : kvStateInformation(kvStateInformation),
          dbPath(instanceRocksDBPath.string()),
          dbOptions(std::move(dbOptions)),
          columnFamilyOptionsFactory(std::move(columnFamilyOptionsFactory)) {
        columnFamilyHandles.reserve(1);
    }

    void openDB()
    {
        loadDb();
    }

    void openDB(
            const std::vector<rocksdb::ColumnFamilyDescriptor>& columnFamilyDescriptors,
            const std::vector<StateMetaInfoSnapshot>& stateMetaInfoSnapshots,
            const fs::path& restoreSourcePath)
    {
        this->columnFamilyDescriptors = columnFamilyDescriptors;
        this->columnFamilyHandles.resize(columnFamilyDescriptors.size() + 1);
        restoreInstanceDirectoryFromPath(restoreSourcePath);
        loadDb();

        for (size_t i = 0; i < stateMetaInfoSnapshots.size(); i++) {
            getOrRegisterStateColumnFamilyHandle(
                columnFamilyHandles[i], stateMetaInfoSnapshots[i]);
        }
    }

    rocksdb::DB* getDb()
    {
        return db;
    }

    rocksdb::ColumnFamilyHandle* getDefaultColumnFamilyHandle()
    {
        return defaultColumnFamilyHandle;
    };
    std::vector<rocksdb::ColumnFamilyHandle*> getColumnFamilyHandles()
    {
        return columnFamilyHandles;
    }

    std::function<rocksdb::ColumnFamilyOptions(const std::string&)> getColumnFamilyOptionsFactory()
    {
        return columnFamilyOptionsFactory;
    }

    std::shared_ptr<rocksdb::DBOptions> getDbOptions()
    {
        return dbOptions;
    }

    std::shared_ptr<RocksDbKvStateInfo> getOrRegisterStateColumnFamilyHandle(
            rocksdb::ColumnFamilyHandle* columnFamilyHandle,
            const StateMetaInfoSnapshot& stateMetaInfoSnapshot)
    {
        std::shared_ptr<RocksDbKvStateInfo> registeredStateMetaInfoEntry = nullptr;
        // Check if the state already exists in the map
        auto it = kvStateInformation->find(stateMetaInfoSnapshot.getName());
        if (it != kvStateInformation->end()) {
            registeredStateMetaInfoEntry = it->second;
        }

        if (nullptr == registeredStateMetaInfoEntry) {
            std::shared_ptr<RegisteredStateMetaInfoBase> stateMetaInfo =
                    RegisteredStateMetaInfoBase::fromMetaInfoSnapshot(stateMetaInfoSnapshot);
            if (columnFamilyHandle == nullptr) {
                registeredStateMetaInfoEntry =
                        RocksDbOperationUtils::createStateInfo(
                            stateMetaInfo,
                            db,
                            columnFamilyOptionsFactory);
            } else {
                registeredStateMetaInfoEntry =
                        std::make_shared<RocksDbKvStateInfo>(columnFamilyHandle, std::move(stateMetaInfo));
            }
            RocksDbOperationUtils::registerKvStateInformation(
                kvStateInformation,
                stateMetaInfoSnapshot.getName(),
                registeredStateMetaInfoEntry);
        }
        return registeredStateMetaInfoEntry;
    }

    void restoreInstanceDirectoryFromPath(const fs::path& source)
    {
        fs::path instanceRocksDBDirectory(dbPath);
        std::error_code ec;
        bool created = fs::create_directories(instanceRocksDBDirectory, ec);
        if (ec || !created) {
            std::string errMsg = "Could not create RocksDB data directory: " + instanceRocksDBDirectory.string();
            std::cerr << "ERROR: " << errMsg << std::endl;
            throw std::runtime_error(errMsg);
        }

        std::vector<fs::directory_entry> entries;
        try {
            for (const auto& entry : fs::directory_iterator(source)) {
                entries.push_back(entry);
            }
        } catch (const fs::filesystem_error& ex) {
            std::string errMsg = "Could not list directory: " + source.string() + ", error: " + ex.what();
            std::cerr << "ERROR: " + errMsg << std::endl;
            throw;
        }
        for (const auto& entry : entries) {
            if (!entry.is_regular_file()) {
                continue;
            }
            const fs::path& file = entry.path();
            const std::string fileName = file.filename().string();
            const fs::path targetFile = instanceRocksDBDirectory / fileName;

            bool hardLinkSuccess = false;

            if (endsWithSst(fileName)) {
                try {
                    fs::create_hard_link(file, targetFile);
                    hardLinkSuccess = true;
                    continue;
                } catch (const fs::filesystem_error& ex) {
                    std::string logMessage = "Could not hard link sst file " + fileName;
                    std::cout << "INFO: " << logMessage << std::endl;
                }
            }
            std::cout << "hardLinkSuccess state: " << hardLinkSuccess << std::endl;
            try {
                fs::copy_file(file, targetFile, fs::copy_options::overwrite_existing);
            } catch (const fs::filesystem_error& ex) {
                std::string errMsg = "Failed to copy file from " + file.string() + " to " + targetFile.string() +
                                     ", error: " + ex.what();
                std::cerr << "ERROR: " << errMsg << std::endl;
                throw;
            }
        }
    }

private:
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>> *kvStateInformation;
    const std::string dbPath;
    std::shared_ptr<rocksdb::DBOptions> dbOptions;
    const std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory;
    std::vector<rocksdb::ColumnFamilyHandle*> columnFamilyHandles;
    std::vector<rocksdb::ColumnFamilyDescriptor> columnFamilyDescriptors;
    rocksdb::DB* db;
    rocksdb::ColumnFamilyHandle* defaultColumnFamilyHandle;
    const std::string SST_FILE_SUFFIX = ".sst";
    std::string::size_type SST_SUFFIX_LENGTH = 4 ;
    void loadDb()
    {
        rocksdb::ColumnFamilyOptions columnFamilyOptions =
                RocksDbOperationUtils::createColumnFamilyOptions(columnFamilyOptionsFactory, "default");

        // This is a temporary bugfix to address the OOM issue.
        // The logic should be moved to RocksDBResourceContainer in the future.
        auto memoryManaged = reinterpret_cast<Boolean*>(Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::USE_MANAGED_MEMORY));
        if (memoryManaged != nullptr && memoryManaged->value) {
            INFO_RELEASE("RocksDB memory managed is enabled,"
                " RocksDBMemoryOptions::calculatedCacheCapacity: " << RocksDBMemoryOptions::calculatedCacheCapacity <<
                " highPriorityPoolRatio: " << RocksDBMemoryOptions::highPriorityPoolRatio <<
                " writeBufferManagerCapacity: " << RocksDBMemoryOptions::writeBufferManagerCapacity)
            auto cache =  ROCKSDB_NAMESPACE::NewLRUCache(RocksDBMemoryOptions::calculatedCacheCapacity, -1, false, RocksDBMemoryOptions::highPriorityPoolRatio);
            std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager> writeBufferManager =
                std::make_shared<ROCKSDB_NAMESPACE::WriteBufferManager>(RocksDBMemoryOptions::writeBufferManagerCapacity, cache);
            dbOptions->write_buffer_manager = writeBufferManager;

            ROCKSDB_NAMESPACE::BlockBasedTableOptions blockBasedTableOptions;
            blockBasedTableOptions.block_cache = cache;
            columnFamilyOptions.table_factory.reset(NewBlockBasedTableFactory(blockBasedTableOptions));

            memoryManaged->putRefCount();
        }

        db = RocksDbOperationUtils::openDB(
            dbPath, columnFamilyDescriptors, columnFamilyHandles, columnFamilyOptions, *dbOptions);
        defaultColumnFamilyHandle = columnFamilyHandles[0];
        columnFamilyHandles.erase(columnFamilyHandles.begin());
    }

    bool endsWithSst(const std::string& str)
    {
        if (str.length() < SST_SUFFIX_LENGTH) {
            return false;
        }
        return str.substr(str.length() - SST_SUFFIX_LENGTH) == SST_FILE_SUFFIX;
    }
};
#endif // OMNISTREAM_ROCKSDBHANDLE_H
