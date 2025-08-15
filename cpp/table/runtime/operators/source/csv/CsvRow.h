/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef CSVROW_H
#define CSVROW_H

#pragma once

#include "CsvNode.h"
#include "CsvSchema.h"
#include <vector>
#include <string>
#include <sstream>

namespace omnistream {
namespace csv {

class CsvRow {
public:
    explicit CsvRow(const std::string &line, const CsvSchema &schema) : innerSchema(schema)
    {
        parseLine(line, schema);
    }

    const std::vector<std::shared_ptr<CsvNode>> &getNodes() const
    {
        return nodes_;
    }

    CsvSchema getSchema() const { return innerSchema; }

private:
    std::vector<std::shared_ptr<CsvNode>> nodes_;
    CsvSchema innerSchema;

    void parseLine(const std::string &line, const CsvSchema &schema)
    {
        char delimiter = schema.getColumnSeparator();
        std::string field;
        bool insideQuotes = false;

        int idx = 0;
        for (size_t i = 0; i < line.size(); ++i) {
            char c = line[i];
            if (c == '"') {
                // Toggle quote mode, or handle escaped quotes
                if (insideQuotes && i + 1 < line.size() && line[i + 1] == '"') {
                    field += '"';  // Escaped quote
                    ++i;
                } else {
                    insideQuotes = !insideQuotes;
                }
            } else if (c == delimiter && !insideQuotes) {
                nodes_.emplace_back(std::make_shared<CsvNode>(field, schema.getTypeAtIdx(idx++)));
                field.clear();
            } else {
                field += c;
            }
        }
        nodes_.emplace_back(std::make_shared<CsvNode>(field, schema.getTypeAtIdx(idx++)));
    }
};

}  // namespace csv
}  // namespace omnistream
#endif
