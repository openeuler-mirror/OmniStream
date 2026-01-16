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
#ifndef FLINK_TNEL_STATEMAPVIEW_H
#define FLINK_TNEL_STATEMAPVIEW_H

#include <nlohmann/json.hpp>
#include "MapView.h"
#include "StateDataView.h"
#include "core/api/common/state/ValueState.h"
#include "core/api/common/state/MapState.h"
#include "../runtime/state/rocksdb/RocksdbMapState.h"
using json = nlohmann::json;



template <typename N, typename EK, typename EV>
class StateMapView : public MapView<EK, EV>, public StateDataView<N> {
public:
    // get a view of cowMap, i.e. copy it
    emhash7::HashMap<EK, EV> *getMap() override
    {
        auto map = MapView<EK, EV>::getMap();
        emhash7::HashMap<EK, EV> *mapView = new emhash7::HashMap<EK, EV>(*map);
        return mapView;
    }

    void setMap() {}
};

template <typename N, typename EK, typename EV>
class StateMapViewWithKeysNullable : public StateMapView<N, EK, EV> {
public:
    std::optional<EV> get(const std::optional<EK>& key) override { return key == std::nullopt ? getNullState()->value() : getMapState()->get(*key); };
    void put(const std::optional<EK>& key, const EV& value) override { key == std::nullopt ? getNullState()->update(value) : getMapState()->put(*key, value); };
    void remove(const std::optional<EK>& key) { key == std::nullopt ? getNullState()->clear() : getMapState()->remove(*key); };
    void contains(const std::optional<EK>& key) { return key == std::nullopt ? getNullState()->value() != nullptr : getMapState()->contains(*key); };
    emhash7::HashMap<EK, EV> *entries()
    {
        return getMapState()->entries();
    };
    void putByBatch(std::vector<std::shared_ptr<std::tuple<RowData*,EK,std::shared_ptr<std::string>>>> & batchData)
    {
        auto rocksDBMap = dynamic_cast<RocksdbMapState<RowData*,N,EK,EV> *>(getMapState());
        if (rocksDBMap) {
            rocksDBMap->putByBatch(batchData);
        }
    }

    std::shared_ptr<json> getInnerMap(EK& ek)
    {
        auto rocksDBMap = dynamic_cast<RocksdbMapState<RowData*,N,EK,EV> *>(getMapState());
        if (rocksDBMap) {
            std::shared_ptr<std::string> rawString= rocksDBMap->getRawBytes(ek);
            try {
                if (rawString == nullptr || rawString->empty()) {
                    return nullptr;
                }
                return std::make_shared<json>(json::parse(*rawString));
            } catch (const json::parse_error& e) {
               LOG("parse json error............");
            }
        }
        return nullptr;
    }

    void cleanup()
    {
        getMapState()->clearEntriesCache();
    }
protected:
    virtual ValueState<EV> *getNullState() = 0;
    virtual MapState<EK, EV> *getMapState() = 0;
};
template <typename N, typename EK, typename EV>
class KeyedStateMapViewWithKeysNullable : public StateMapViewWithKeysNullable<N, EK, EV> {
public:
    KeyedStateMapViewWithKeysNullable(MapState<EK, EV> *mapState, ValueState<EV> *nullState) : mapState(mapState), nullState(nullState) {};
    void setCurrentNamespace(N nameSpace) override { };
    void clear() override
    {
        getMapState()->clear();
        getNullState()->clear();
    };
protected:
    MapState<EK, EV> *getMapState() override { return mapState; };
    ValueState<EV> *getNullState() override { return nullState; };

private:
    MapState<EK, EV> *mapState;
    ValueState<EV> *nullState;
};
template <typename N, typename EK, typename EV>
class StateMapViewWithKeysNotNull : public StateMapView<N, EK, EV> {
public:
    std::optional<EV> get(const std::optional<EK>& key) override
    {
        return getMapState()->get(*key);
    };
    void put(const std::optional<EK>& key, const EV& value) override { getMapState()->put(*key, value); };
    void remove(const std::optional<EK>& key) { getMapState()->remove(*key); };
    bool contains(const std::optional<EK>& key) { return getMapState()->contains(*key); };
    void clear() override { getMapState()->clear(); };

protected:
    virtual MapState<EK, EV> *getMapState() = 0;
private:
};

template <typename N, typename EK, typename EV>
class KeyedStateMapViewWithKeysNotNull : public StateMapViewWithKeysNotNull<N, EK, EV> {
public:
    explicit KeyedStateMapViewWithKeysNotNull(MapState<EK, EV> *mapState) : mapState(mapState) {
    };

    void setCurrentNamespace(N nameSpace) override
    {
        std::runtime_error("Unsupported");
    }

protected:
    MapState<EK, EV> *getMapState() override { return mapState; };
private:
    MapState<EK, EV> *mapState;
};

#endif // FLINK_TNEL_STATEMAPVIEW_H
