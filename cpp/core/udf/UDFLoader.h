/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_UDFLOADER_H
#define FLINK_TNEL_UDFLOADER_H

#include <string>
#include "dlfcn.h"
#include "functions/MapFunction.h"
#include "functions/ReduceFunction.h"
#include "functions/FilterFunction.h"
#include "functions/SourceFunction.h"
#include "functions/FlatMapFunction.h"
#include "functions/KeySelect.h"
#include "functions/RuntimeContext.h"
#include "udf_hash.h"
#include "nlohmann/json.hpp"

using MapDllType = MapFunctionUnique<Object>(nlohmann::json);
using ReduceDllType = ReduceFunctionUnique<Object>(nlohmann::json);
using FlatMapDllType = FlatMapFunctionUnique<Object>(nlohmann::json);
using FilterDllType = FilterFunctionUnique<Object>(nlohmann::json);
using SourceDllType = SourceFunctionUnique<Object>(nlohmann::json);
using KeySelectDllType = KeySelectUnique<Object>(nlohmann::json);


using SerializeFunction = char*(char*);
using DeSerializeFunction = char*(char*);
using DebugFunction = void(char*);

using RichMapFunctionType = MapFunctionUnique<Object>(RuntimeContext *);

enum class UDFLogicType : uint8_t {
    Map,
    Filter,
    RichMap,
    RichFilter,
    Serialize,
    DeSerialize,
    Reduce,
    Hash,
    Cmp,
};

class UDFLoader {
public:
    MapDllType* LoadMapFunction(const std::string &filePath)
    {
        return LoadUDFFunction<MapDllType >(filePath, MapFuncName);
    }

    FlatMapDllType* LoadFlatMapFunction(const std::string &filePath)
    {
        return LoadUDFFunction<FlatMapDllType >(filePath, FlatMapFuncName);
    }

    FilterDllType* LoadFilterFunction(const std::string &filePath)
    {
        return LoadUDFFunction<FilterDllType >(filePath, FilterFuncName);
    }

    ReduceDllType *LoadReduceFunction(const std::string &filePath)
    {
        return LoadUDFFunction<ReduceDllType>(filePath, ReduceFuncName);
    }

    SerializeFunction *LoadSerFunction(const std::string &filePath)
    {
        return LoadUDFFunction<SerializeFunction>(filePath, SerializeName);
    }

    SerializeFunction *LoadDeSerFunction(const std::string &filePath)
    {
        return LoadUDFFunction<DeSerializeFunction>(filePath, DeSerializeName);
    }

    DebugFunction* LoadDebugFunction(const std::string &filePath)
    {
        return LoadUDFFunction<DebugFunction>(filePath, DebugName);
    }

    HashFunctionType* LoadHashFunction(const std::string &filePah)
    {
        return LoadUDFFunction<HashFunctionType>(filePah, HashName);
    }

    CmpFunctionType* LoadCmpFunction(const std::string &filePah)
    {
        return LoadUDFFunction<CmpFunctionType>(filePah, CmpName);
    }

    SourceDllType* LoadSourceFunction(const std::string &filePath)
    {
        return LoadUDFFunction<SourceDllType >(filePath, SourceFuncName);
    }

    KeySelectDllType* LoadKeySelectFunction(const std::string &filePath)
    {
        return LoadUDFFunction<KeySelectDllType >(filePath, KeySelectName);
    }


    RichMapFunctionType* LoadRichMapFunction(const std::string &filePah)
    {
        return LoadUDFFunction<RichMapFunctionType>(filePah, HashName);
    }

private:
    template<typename FuncType>
    FuncType* LoadUDFFunction(const std::string &filePath, const std::string &funcSignature)
    {
        void* handle = dlopen(filePath.c_str(), RTLD_LAZY);
        if (not handle) {
            std::cerr << "Error loading library: " << dlerror() << std::endl;
            return nullptr;
        }

        FuncType *funcPointer = (FuncType *)dlsym(handle, funcSignature.c_str());

        const char* path = nullptr;
        if (path == nullptr) {
            Dl_info info;
            if (dladdr((void*)funcPointer, &info)) {
                path = info.dli_fname;
            } else {
                path = "[Error] Failed to get SO path";
            }
        }
        std::cout << "so path: " << path << std::endl;

        const char* error = dlerror();
        if (error) {
            std::cerr << "Error finding symbol: " << error << std::endl;
            dlclose(handle);
            return nullptr;
        }
        return funcPointer;
    }

    const char* ReduceFuncName = "NewInstance";
    const char* MapFuncName = "NewInstance";
    const char* SerializeName = "NewInstance";
    const char* DeSerializeName = "NewInstance";
    const char* FlatMapFuncName = "NewInstance";
    const char* FilterFuncName = "NewInstance";
    const char* SourceFuncName = "NewInstance";
    const char* KeySelectName = "NewInstance";
    const char* DebugName = "NewInstance";
    const char* HashName = "Hash";
    const char* CmpName = "Cmp";
};
#endif //FLINK_TNEL_UDFLOADER_H
