/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef RESOURCEIDPOD_H
#define RESOURCEIDPOD_H

#include <string>
#include <nlohmann/json.hpp>

namespace omnistream {

    class ResourceIDPOD {
    public:
        // Default constructor
        ResourceIDPOD() : resourceId(""), metadata("") {}

        // Full argument constructor
        ResourceIDPOD(const std::string& resourceId, const std::string& metadata)
            : resourceId(resourceId), metadata(metadata) {}

        friend bool operator==(const ResourceIDPOD& lhs, const ResourceIDPOD& rhs)
        {
            return lhs.resourceId == rhs.resourceId
                && lhs.metadata == rhs.metadata;
        }

        friend bool operator!=(const ResourceIDPOD& lhs, const ResourceIDPOD& rhs)
        {
            return !(lhs == rhs);
        }

        friend std::size_t hash_value(const ResourceIDPOD& obj)
        {
            std::size_t seed = 0x73134921;
            seed ^= (seed << 6) + (seed >> 2) + 0x79ED6714 + std::hash<std::string>()(obj.resourceId);
            seed ^= (seed << 6) + (seed >> 2) + 0x125B0018 + std::hash<std::string>()(obj.metadata);
            return seed;
        }

        // Getters
        const std::string& getResourceId() const { return resourceId; }
        const std::string& getMetadata() const { return metadata; }

        // Setters
        void setResourceId(const std::string& resourceId) { this->resourceId = resourceId; }
        void setMetadata(const std::string& metadata) { this->metadata = metadata; }

        // toString method
        std::string toString() const
        {
            return "ResourceIDPOD { resourceId: \"" + resourceId + "\", metadata: \"" + metadata + "\" }";
        }

        // JSON serialization/deserialization using NLOHMANN_DEFINE_TYPE_INTRUSIVE
        NLOHMANN_DEFINE_TYPE_INTRUSIVE(ResourceIDPOD, resourceId, metadata)
    private:
        std::string resourceId;
        std::string metadata;
    };

} // namespace omnistream

namespace std
{
    template <>
    struct hash<omnistream::ResourceIDPOD> {
        std::size_t operator()(const omnistream::ResourceIDPOD& obj) const
        {
            return hash_value(obj);
        }
    };
}


#endif // RESOURCEIDPOD_H

