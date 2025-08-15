/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/17/25.
//

#ifndef TYPEDESCRIPTIONPOD_H
#define TYPEDESCRIPTIONPOD_H


#include <string>
#include <nlohmann/json.hpp>

namespace omnistream {

    class TypeDescriptionPOD {
    public:
        std::string kind;
        bool isNull;
        int precision;
        std::string type;
        int timestampKind;
        std::string fieldName;

        TypeDescriptionPOD() : kind(""), isNull(false), precision(0), type(""), timestampKind(0), fieldName("") {}

        TypeDescriptionPOD(const std::string& kind, bool isNull, int precision, const std::string& type, int timestampKind, std::string fieldName)
            : kind(kind), isNull(isNull), precision(precision), type(type), timestampKind(timestampKind), fieldName(fieldName) {}

        TypeDescriptionPOD(const TypeDescriptionPOD& other) = default;

        std::string getKind() const { return kind; }
        void setKind(const std::string& kind) { this->kind = kind; }

        bool getIsNull() const { return isNull; }
        void setIsNull(bool isNull) { this->isNull = isNull; }

        int getPrecision() const { return precision; }
        void setPrecision(int precision) { this->precision = precision; }

        std::string getType() const { return type; }
        void setType(const std::string& type) { this->type = type; }

        int getTimestampKind() const { return timestampKind; }
        void setTimestampKind(int timestampKind) { this->timestampKind = timestampKind; }

        std::string getFieldName() const { return fieldName; }
        void setFieldName(const std::string& fieldName) { this->fieldName = fieldName; }

        std::string toString() const
        {
            return "TypeDescriptionPOD{kind='" + kind + "', isNull=" + std::to_string(isNull) +
                   ", precision=" + std::to_string(precision) + ", type='" + type +
                   "', timestampKind=" + std::to_string(timestampKind) +
                   "', fieldName=" + fieldName + "}";
        }

        bool operator==(const TypeDescriptionPOD&other)const
        {
            return
            this->kind==other.kind &&
                this->isNull==other.isNull &&
                    this->precision==other.precision&&
                        this->type==other.type &&
                            this->timestampKind==other.timestampKind &&
                                this->fieldName==other.fieldName;
        }

        NLOHMANN_DEFINE_TYPE_INTRUSIVE(TypeDescriptionPOD, kind, isNull, precision, type, timestampKind, fieldName)
    };

} // namespace omnistream

namespace std {
    template <>
    struct hash<omnistream::TypeDescriptionPOD> {
        std::size_t operator()(const omnistream::TypeDescriptionPOD& obj) const
        {
            size_t h1=std::hash<std::string>{}(obj.kind);
            size_t h2=std::hash<long>{}(obj.isNull);
            size_t h3=std::hash<int>{}(obj.precision);
            size_t h4=std::hash<std::string>{}(obj.type);
            size_t h5=std::hash<int>{}(obj.timestampKind);
            size_t h8=std::hash<std::string>{}(obj.fieldName);

            size_t seed = 0;

            seed ^= (h1 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
            seed ^= (h2 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
            seed ^= (h3 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
            seed ^= (h4 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
            seed ^= (h5 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
            seed ^= (h8 + 0x9e3779b9 + (seed << 6) + (seed >> 2));

            return seed;
        }
    };
} // namespace std


#endif //TYPEDESCRIPTIONPOD_H
