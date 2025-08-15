/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/11/25.
//

#ifndef JOBIDPOD_H
#define JOBIDPOD_H

#include <iostream>
#include <string>
#include <nlohmann/json.hpp> // Include for JSON serialization

namespace omnistream {

    class JobIDPOD {
    public:
        JobIDPOD() : upperPart(0), lowerPart(0) {} // Default constructor
        JobIDPOD(long long upper, long long lower) : upperPart(upper), lowerPart(lower) {}

        JobIDPOD(const JobIDPOD &other)
            : upperPart(other.upperPart),
              lowerPart(other.lowerPart) {
        }

        JobIDPOD(JobIDPOD &&other) noexcept
            : upperPart(other.upperPart),
              lowerPart(other.lowerPart) {
        }

        JobIDPOD& operator=(const JobIDPOD &other)
        {
            if (this == &other) {
                return *this;
            }
            upperPart = other.upperPart;
            lowerPart = other.lowerPart;
            return *this;
        }

        JobIDPOD& operator=(JobIDPOD &&other) noexcept
        {
            if (this == &other) {
                return *this;
            }
            upperPart = other.upperPart;
            lowerPart = other.lowerPart;
            return *this;
        }

        friend bool operator==(const JobIDPOD& lhs, const JobIDPOD& rhs)
        {
            return lhs.upperPart == rhs.upperPart
                && lhs.lowerPart == rhs.lowerPart;
        }

        friend bool operator!=(const JobIDPOD& lhs, const JobIDPOD& rhs)
        {
            return !(lhs == rhs);
        }

        friend std::size_t hash_value(const JobIDPOD& obj)
        {
            std::size_t seed = 0x3DD43D4E;
            seed ^= (seed << 6) + (seed >> 2) + 0x4218D36C + static_cast<std::size_t>(obj.upperPart);
            seed ^= (seed << 6) + (seed >> 2) + 0x7AA0C6A1 + static_cast<std::size_t>(obj.lowerPart);
            return seed;
        }

        long long getUpperPart() const { return upperPart; }
        void setUpperPart(long long upper) { upperPart = upper; }

        long long getLowerPart() const { return lowerPart; }
        void setLowerPart(long long lower) { lowerPart = lower; }

        std::string toString() const
        {
            return "JobIDPOD{ upperPart=" + std::to_string(upperPart) +
                   ", lowerPart=" + std::to_string(lowerPart) +
                   '}';
        }

        NLOHMANN_DEFINE_TYPE_INTRUSIVE(JobIDPOD, upperPart, lowerPart)
    private:
        long long upperPart;
        long long lowerPart;
    };


} // namespace omnistream

namespace std {
    template <>
    struct hash<omnistream::JobIDPOD> {
        std::size_t operator()(const omnistream::JobIDPOD& obj) const
        {
            return hash_value(obj);
        }
    };
}


#endif //JOBIDPOD_H
