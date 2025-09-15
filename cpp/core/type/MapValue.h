/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_MAPVALUE_H
#define FLINK_TNEL_MAPVALUE_H

#include <typeinfo>
#include <string>
#include <emhash7.hpp>
#include <stdexcept>
#include "../io/IOReadableWritable.h"

template<typename K, typename V>
class MapValue : public IOReadableWritable {
public:
    void write(DataOutputSerializer &out) override;

    void read(DataInputView &in) override;
private:
    //todo: replace this with better cowMap
    emhash7::HashMap<K, V> map;
};


template<typename K, typename V>
inline void MapValue<K, V>::read(DataInputView &in)
{
    int size = in.readInt();
    map.clear();

    //     //todo: only work for int type, will improve later
    //     if constexpr (std::is_same_v<K, int> || std::is_same_v<V, int>)  {
    //             cowMap[key] = value;
    //         }
    //     }

    // }
}

template<typename K, typename V>
inline void MapValue<K, V>::write(DataOutputSerializer &out)
{
    out.writeInt(map.size());
    //todo: only work for int type, will improve later
    // if constexpr (std::is_same_v<K, int> || std::is_same_v<V, int>)  {
    //     }
    //     throw std::runtime_error("key or value type not supported!");
    // }
}


#endif //FLINK_TNEL_MAPVALUE_H
