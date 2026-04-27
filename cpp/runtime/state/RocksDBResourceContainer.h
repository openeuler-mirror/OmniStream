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
#ifndef OMNISTREAM_ROCKSDBRESOURCECONTAINER_H
#define OMNISTREAM_ROCKSDBRESOURCECONTAINER_H

#include "RocksDBSharedResources.h"

class RocksDBResourceContainer {
public:
    RocksDBResourceContainer(std::shared_ptr<RocksDBSharedResources> sharedResources, const fs::path& instanceBasePath, bool enableStatistics)
        : sharedResources_(sharedResources), enableStatistics_(enableStatistics)
    {
        if (!instanceBasePath.empty()) {
            instanceRocksDBPath_ = instanceBasePath / "db";
        }
    }

    ~RocksDBResourceContainer() = default;

    std::shared_ptr<rocksdb::DBOptions> getDbOptions()
    {
        auto options = createBaseCommonDBOptions();

        // set the configurable options
        DefaultConfigurableOptionsFactory::createDBOptions(*options);

        if (sharedResources_ != nullptr) {
            options->write_buffer_manager = sharedResources_->getWriteBufferManager();
        } else {
            THROW_LOGIC_EXCEPTION("RocksDBResourceContainer::getDbOptions, RocksDBSharedResources should not be null.");
        }

        return options;
    }

    std::shared_ptr<rocksdb::ColumnFamilyOptions> getColumnOptions() {
        auto columnFamilyOptions = createBaseCommonColumnOptions();
        ROCKSDB_NAMESPACE::BlockBasedTableOptions blockBasedTableOptions;
        blockBasedTableOptions.block_cache = sharedResources_->getCache();
        DefaultConfigurableOptionsFactory::createColumnOptions(*columnFamilyOptions, blockBasedTableOptions);
        return columnFamilyOptions;
    }

private:
    std::shared_ptr<RocksDBSharedResources> sharedResources_;
    std::optional<fs::path> instanceRocksDBPath_;
    bool enableStatistics_;

    std::shared_ptr<rocksdb::DBOptions> createBaseCommonDBOptions()
    {
        auto options = std::make_shared<rocksdb::DBOptions>();
        options->use_fsync = false;
        options->stats_dump_period_sec = 0;
        options->create_if_missing = true;
        return options;
    }

    std::shared_ptr<rocksdb::ColumnFamilyOptions> createBaseCommonColumnOptions() {
        auto opt = std::make_shared<rocksdb::ColumnFamilyOptions>();
        return opt;
    }
};

#endif // OMNISTREAM_ROCKSDBRESOURCECONTAINER_H
