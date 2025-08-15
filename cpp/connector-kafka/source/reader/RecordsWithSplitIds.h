/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_RECORDSWITHSPLITIDS_H
#define FLINK_TNEL_RECORDSWITHSPLITIDS_H

#include <string>
#include <set>
#include <memory>
#include <optional>
#include <vector>

template <typename E>
class RecordsWithSplitIds {
public:
    // 移动到下一个分片，若没有分片则返回空指针
    virtual std::string nextSplit() = 0;
    // 从当前分片中获取下一条记录，若当前分片没有更多记录则返回空指针
    virtual const E* nextRecordFromSplit() = 0;
    virtual const std::vector<E*>& getRecordsFromSplit() = 0;
    virtual size_t getSplitStoppingOffset() = 0;
    // 获取已完成的分片
    virtual std::set<std::string> &finishedSplits() = 0;
    // 当这批记录全部发出时调用，默认实现为空
    virtual void recycle() {}
    // 虚析构函数，确保派生类对象能被正确释放
    virtual ~RecordsWithSplitIds() = default;
};

#endif // FLINK_TNEL_RECORDSWITHSPLITIDS_H

