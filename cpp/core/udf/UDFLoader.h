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
#include "functions/KeyedCoProcessFunction.h"
#include "streaming/api/functions/ProcessFunction.h"
#include "functions/RuntimeContext.h"
#include "udf_hash.h"
#include "nlohmann/json.hpp"

using MapDllType = MapFunctionUnique<Object>(nlohmann::json);
using ReduceDllType = ReduceFunctionUnique<Object>(nlohmann::json);
using FlatMapDllType = FlatMapFunctionUnique<Object>(nlohmann::json);
using FilterDllType = FilterFunctionUnique<Object>(nlohmann::json);
using SourceDllType = SourceFunctionUnique<Object>(nlohmann::json);
using KeySelectDllType = KeySelectUnique<Object>(nlohmann::json);
using KeyedCoProcessDllType = KeyedCoProcessFunctionUnique<Object*, Object*, Object*, Object*>(nlohmann::json);
using ProcessOperatorDllType = ProcessFunctionUnique<Object*, Object*>(nlohmann::json);

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

    KeyedCoProcessDllType* LoadKeyedCoProcessFunction(const std::string &filePath)
    {
        return LoadUDFFunction<KeyedCoProcessDllType >(filePath, KeyedCoProcessFuncName);
    }

    ProcessOperatorDllType* LoadProcessOperatorFunction(const std::string &filePath)
    {
        return LoadUDFFunction<ProcessOperatorDllType >(filePath, ProcessOperatorFuncName);
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

    const char* NormalFunctionName = "NewInstance";
    const char* ReduceFuncName = NormalFunctionName;
    const char* MapFuncName = NormalFunctionName;
    const char* SerializeName = NormalFunctionName;
    const char* DeSerializeName = NormalFunctionName;
    const char* FlatMapFuncName = NormalFunctionName;
    const char* FilterFuncName = NormalFunctionName;
    const char* SourceFuncName = NormalFunctionName;
    const char* KeySelectName = NormalFunctionName;
    const char* KeyedCoProcessFuncName = NormalFunctionName;
    const char* ProcessOperatorFuncName = NormalFunctionName;
    const char* DebugName = NormalFunctionName;
    const char* HashName = "Hash";
    const char* CmpName = "Cmp";
};
#endif
