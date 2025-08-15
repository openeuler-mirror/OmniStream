/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/26/25.
//

#ifndef INTERMEDIATEDATASETIDPOD_H
#define INTERMEDIATEDATASETIDPOD_H
#include "AbstractIDPOD.h"
#include "table/data/RowData.h"

namespace omnistream
{
    class IntermediateDataSetIDPOD : public  AbstractIDPOD {
    public:
        IntermediateDataSetIDPOD() : AbstractIDPOD() {};

        IntermediateDataSetIDPOD(long upper, long lower)
            : AbstractIDPOD(upper, lower)
        {
        }

        IntermediateDataSetIDPOD(const IntermediateDataSetIDPOD& other)
            : AbstractIDPOD(other)
        {
        }

        IntermediateDataSetIDPOD(IntermediateDataSetIDPOD&& other) noexcept
            : AbstractIDPOD(std::move(other))
        {
        }

        ~IntermediateDataSetIDPOD() = default;

        IntermediateDataSetIDPOD& operator=(const IntermediateDataSetIDPOD& other)
        {
            if (this == &other) {
                return *this;
            }
            AbstractIDPOD::operator =(other);
            return *this;
        }

        IntermediateDataSetIDPOD& operator=(IntermediateDataSetIDPOD&& other) noexcept
        {
            if (this == &other) {
                return *this;
            }
            AbstractIDPOD::operator =(std::move(other));
            return *this;
        }

        bool operator==(const IntermediateDataSetIDPOD& other) const
        {
            return upperPart == other.upperPart && lowerPart == other.lowerPart;
        }

        bool operator==(const IntermediateDataSetIDPOD& other)
        {
            return upperPart == other.upperPart && lowerPart == other.lowerPart;
        }

        std::size_t operator()(const IntermediateDataSetIDPOD& p)
        {
            std::size_t seed = 0;
            seed = hash_combine(seed, p.upperPart);
            return hash_combine(seed, p.lowerPart);
        }

        std::size_t operator()(const IntermediateDataSetIDPOD& p) const
        {
            std::size_t seed = 0;
            seed = hash_combine(seed, p.upperPart);
            return hash_combine(seed, p.lowerPart);
        }
    };
}


namespace std {
    template <>
    struct hash<omnistream::IntermediateDataSetIDPOD> {
        size_t operator()(const omnistream::IntermediateDataSetIDPOD& intermediateDataSetID) const
        {
            std::size_t h1 = std::hash<int>()(intermediateDataSetID.getUpperPart());
            std::size_t h2 = std::hash<int>()(intermediateDataSetID.getLowerPart());
            return h1 ^ (h2 << 1);
        }
    };
}

#endif //INTERMEDIATEDATASETIDPOD_H
