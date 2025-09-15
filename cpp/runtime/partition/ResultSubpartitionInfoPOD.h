/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef RESULT_SUBPARTITION_INFO_POD_H
#define RESULT_SUBPARTITION_INFO_POD_H

#include <string>

namespace omnistream {

    class ResultSubpartitionInfoPOD {
    public:
        ResultSubpartitionInfoPOD();
        ResultSubpartitionInfoPOD(int partitionIdx, int subPartitionIdx);
        ResultSubpartitionInfoPOD(const ResultSubpartitionInfoPOD& other);
        ~ResultSubpartitionInfoPOD();

        int getPartitionIdx() const;
        void setPartitionIdx(int partitionIdx);

        int getSubPartitionIdx() const;
        void setSubPartitionIdx(int subPartitionIdx);

        bool operator==(const ResultSubpartitionInfoPOD& other) const;
        bool operator!=(const ResultSubpartitionInfoPOD& other) const;

        std::string toString() const;

    private:
        int partitionIdx;
        int subPartitionIdx;
    };

} // namespace omnistream

namespace std {
    template <>
    struct hash<omnistream::ResultSubpartitionInfoPOD> {
        size_t operator()(const omnistream::ResultSubpartitionInfoPOD& obj) const {
            size_t seed = 0;
            // Example: Hashing based on the members of ResultSubpartitionInfoPOD
            // Modify this based on your actual members
            size_t h1 = std::hash<int>()(obj.getPartitionIdx());
            size_t h2 = std::hash<int>()(obj.getSubPartitionIdx());
            // Combine the hashes using a technique such as XOR and prime numbers
            seed ^= h1 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            seed ^= h2 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            return seed;
        }
    };
}


#endif // RESULT_SUBPARTITION_INFO_POD_H