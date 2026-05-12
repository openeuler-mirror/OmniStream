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

#ifndef OMNISTREAM_ADDSPLITSTASK_H
#define OMNISTREAM_ADDSPLITSTASK_H

#include "SplitFetcherTask.h"
#include <unordered_map>
#include "connector/kafka/source/reader/SplitReader.h"

template <typename E, typename SplitT>
class AddSplitsTask : public SplitFetcherTask {
public:
    AddSplitsTask(
            SplitReader<RdKafka::Message, KafkaPartitionSplit> *splitReader,
            std::vector<KafkaPartitionSplit*>& splitsToAdd,
            std::unordered_map<std::string, KafkaPartitionSplit*>& assignedSplits): splitReader(splitReader),
                                                                                    splitsToAdd(splitsToAdd),
                                                                                    assignedSplits(assignedSplits) {}

    ~AddSplitsTask() override
    {
    }

    bool Run() override
    {
        for (auto& s : splitsToAdd) {
            // std::cout << "AddSplitsTask splitId:" << s->splitId() << std::endl;
            assignedSplits.emplace(s->splitId(), s);
        }
        splitReader->handleSplitsChanges(splitsToAdd);
        return true;
    }

    void WakeUp() override {}

    std::string ToString() override
    {
        std::string result = "AddSplitsTask: [";
        for (size_t i = 0; i < splitsToAdd.size(); ++i) {
            if (i > 0) {
                result += ", ";
            }
            // 这里假设 SplitT 有合适的输出方式，可按需修改
            result += splitsToAdd[i]->splitId();
            result += " : ";
            result += std::to_string(splitsToAdd[i]->getStartingOffset());
            result += " - ";
            result += std::to_string(splitsToAdd[i]->getStoppingOffset());
        }
        result += "]";
        return result;
    }
private:
    SplitReader<E, SplitT>* splitReader;
    std::vector<SplitT*> splitsToAdd;
    std::unordered_map<std::string, SplitT*>& assignedSplits;
};


#endif // OMNISTREAM_ADDSPLITSTASK_H
