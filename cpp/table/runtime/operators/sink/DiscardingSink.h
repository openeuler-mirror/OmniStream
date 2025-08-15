/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_DISCARDINGSINK_H
#define FLINK_TNEL_DISCARDINGSINK_H

#include <regex>
#include "SinkFunction.h"

class DiscardingSink : public SinkFunction<StreamRecord *> {
public:
    explicit DiscardingSink(const nlohmann::json& sinkDescription)
        :outfile(sinkDescription.contains("outputfile") ? sinkDescription["outputfile"] : "")
    {
        if (sinkDescription.contains("inputTypes")) {
            std::regex pattern(R"(DECIMAL\d+\((\d+),\s*(\d+)\))");
            std::smatch match;

            inputTypes = sinkDescription["inputTypes"].get<std::vector<std::string>>();
            for (const std::string& inputType : inputTypes) {
                if (std::regex_search(inputType, match, pattern)) {
                    int precision = std::stoi(match[1].str());
                    int scale = std::stoi(match[2].str());
                    decimalInfo.emplace_back(precision, scale);
                } else {
                    decimalInfo.emplace_back(-1, -1);
                }
            }
        }
    };
    void invoke(StreamRecord *data, SinkInputValueType valueType) override
    {
        if (valueType == SinkInputValueType::ROW_DATA) {
        } else if (valueType == SinkInputValueType::VEC_BATCH) {
            if (outfile == "") {
                omniruntime::vec::VectorHelper::FreeVecBatch(
                    reinterpret_cast<omnistream::VectorBatch *>(data->getValue()));
                delete data;
                return;
            }
            auto vb = reinterpret_cast<omnistream::VectorBatch*>(data->getValue());
            vb->writeToFile(outfile, std::ios::app, decimalInfo, inputTypes);

            delete static_cast<omnistream::VectorBatch*>(data->getValue());
            delete data;
        }
    };
    void writeWatermark(Watermark *watermark) override
    {
        delete watermark;
    };
    void finish() override{};
private:
    std::vector<std::string> inputTypes;
    std::string outfile;
    std::vector<std::pair<int32_t, int32_t>> decimalInfo; // precision, scale
};

#endif  // FLINK_TNEL_DISCARDINGSINK_H
