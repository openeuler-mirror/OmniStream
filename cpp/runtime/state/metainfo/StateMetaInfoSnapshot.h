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

#ifndef OMNISTREAM_STATEMETAINFOSNAPSHOT_H
#define OMNISTREAM_STATEMETAINFOSNAPSHOT_H
#include <string>
#include <unordered_map>
#include <memory>
#include "core/typeutils/TypeSerializer.h"
#include "core/typeutils/TypeSerializerSnapshot.h"
/**
 * Generalized snapshot for meta information about one state in a state backend (e.g. {@link
 * RegisteredKeyValueStateBackendMetaInfo}).
 */
class StateMetaInfoSnapshot {
public:
    /** Enum that defines the different types of state that live in Flink backends. */
    enum class BackendStateType {
        KEY_VALUE = 0,
        OPERATOR = 1,
        BROADCAST = 2,
        PRIORITY_QUEUE = 3
    };

    /** Predefined keys for the most common options in the meta info. */
    enum class CommonOptionsKeys {
        /** Key to define the StateDescriptor::Type of a key/value keyed-state */
        KEYED_STATE_TYPE,
        /**
         * Key to define org::apache::flink::runtime::state::OperatorStateHandle::Mode, about how
         * operator state is distributed on restore
         */
        OPERATOR_STATE_DISTRIBUTION_MODE,
    };

    /** Predefined keys for the most common serializer types in the meta info. */
    enum class CommonSerializerKeys {
        KEY_SERIALIZER,
        NAMESPACE_SERIALIZER,
        VALUE_SERIALIZER
    };

    StateMetaInfoSnapshot(
        const std::string& name,
        BackendStateType backendStateType,
        const std::unordered_map<std::string, std::string>& options,
        const std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>& serializerSnapshots)
        : StateMetaInfoSnapshot(name, backendStateType, options, serializerSnapshots,
            std::unordered_map<std::string, TypeSerializer*>()) {}

    StateMetaInfoSnapshot(
        const std::string& name,
        BackendStateType backendStateType,
        const std::unordered_map<std::string, std::string>& options,
        const std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>& serializerSnapshots,
        const std::unordered_map<std::string, TypeSerializer*>& serializers)
        : name(name),
        backendStateType(backendStateType),
        options(options),
        serializerSnapshots(serializerSnapshots),
        serializers(serializers) {}

    BackendStateType getBackendStateType() const
    {
        return backendStateType;
    }

    std::shared_ptr<TypeSerializerSnapshot> getTypeSerializerSnapshot(const std::string& key) const
    {
        auto it = serializerSnapshots.find(key);
        if (it != serializerSnapshots.end()) {
            return it->second;
        }
        return nullptr;
    }

    std::shared_ptr<TypeSerializerSnapshot> getTypeSerializerSnapshot(CommonSerializerKeys key) const
    {
        return getTypeSerializerSnapshot(commonSerializerKeyToString(key));
    }

    std::string getOption(const std::string& key) const
    {
        auto it = options.find(key);
        if (it != options.end()) {
            return it->second;
        }
        return "";  // Return empty string for null equivalent
    }

    std::string getOption(CommonOptionsKeys key) const
    {
        return getOption(commonOptionsKeyToString(key));
    }

    const std::unordered_map<std::string, std::string>& getOptionsImmutable() const
    {
        return options;
    }

    const std::string& getName() const
    {
        return name;
    }

    const std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>& getSerializerSnapshotsImmutable()
        const
    {
        return serializerSnapshots;
    }

    /** tO-DO this method should be removed once the serializer map is removed. */
    TypeSerializer* getTypeSerializer(const std::string& key) const
    {
        auto it = serializers.find(key);
        if (it != serializers.end()) {
            return it->second;
        }
        return nullptr;
    }

    // Static helper methods for BackendStateType enum
    static unsigned char getCode(BackendStateType type)
    {
        return static_cast<unsigned char>(type);
    }

    static BackendStateType byCode(int code)
    {
        switch (code) {
            case 0: return BackendStateType::KEY_VALUE;
            case 1: return BackendStateType::OPERATOR;
            case 2: return BackendStateType::BROADCAST;
            case 3: return BackendStateType::PRIORITY_QUEUE;
            default:
                throw std::invalid_argument("Unknown BackendStateType: " + std::to_string(code));
        }
    }

    // Helper methods to convert enums to strings
    static std::string commonOptionsKeyToString(CommonOptionsKeys key)
    {
        switch (key) {
            case CommonOptionsKeys::KEYED_STATE_TYPE:
                return "KEYED_STATE_TYPE";
            case CommonOptionsKeys::OPERATOR_STATE_DISTRIBUTION_MODE:
                return "OPERATOR_STATE_DISTRIBUTION_MODE";
            default:
                return "";
        }
    }

    static std::string commonSerializerKeyToString(CommonSerializerKeys key)
    {
        switch (key) {
            case CommonSerializerKeys::KEY_SERIALIZER:
                return "KEY_SERIALIZER";
            case CommonSerializerKeys::NAMESPACE_SERIALIZER:
                return "NAMESPACE_SERIALIZER";
            case CommonSerializerKeys::VALUE_SERIALIZER:
                return "VALUE_SERIALIZER";
            default:
                return "";
        }
    }

    std::string getSerializerJson() const {
        nlohmann::json mapJson;
        for (auto it = serializers.begin(); it != serializers.end(); it++) {
            auto serializer = it->second;
            if (serializer == nullptr) {
                continue;
            }
           mapJson[it->first] = serializer->toJson();
        }
        return mapJson.dump();
    }

private:
    /** The name of the state. */
    const std::string name;

    const BackendStateType backendStateType;

    /** Map of options (encoded as strings) for the state. */
    const std::unordered_map<std::string, std::string> options;

    /** The configurations of all the type serializers used with the state. */
    const std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>> serializerSnapshots;

    // tO-DO this will go away once all serializers have the restoreSerializer() factory method
    // properly implemented.
    /** The serializers used by the state. */
    const std::unordered_map<std::string, TypeSerializer*> serializers;
};
#endif // OMNISTREAM_STATEMETAINFOSNAPSHOT_H
