#ifndef FLINK_TNEL_STATEMAPVIEW_H
#define FLINK_TNEL_STATEMAPVIEW_H
#include "MapView.h"
#include "StateDataView.h"
#include "../../core/api/ValueState.h"
#include "../../core/api/MapState.h"

template <typename N, typename EK, typename EV>
class StateMapView : public MapView<EK, EV>, public StateDataView<N>
{
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
class StateMapViewWithKeysNullable : public StateMapView<N, EK, EV>
{
protected:
    virtual ValueState<EV> *getNullState() = 0;
    virtual MapState<EK, EV> *getMapState() = 0;

public:
    std::optional<EV> get(const std::optional<EK>& key) override { return key == std::nullopt ? getNullState()->value() : getMapState()->get(*key); };
    void put(const std::optional<EK>& key, const EV& value) override { key == std::nullopt ? getNullState()->update(value) : getMapState()->put(*key, value); };
    void remove(const std::optional<EK>& key) { key == std::nullopt ? getNullState()->clear() : getMapState()->remove(*key); };
    void contains(const std::optional<EK>& key) { return key == std::nullopt ? getNullState()->value() != nullptr : getMapState()->contains(*key); };
};
template <typename N, typename EK, typename EV>
class KeyedStateMapViewWithKeysNullable : public StateMapViewWithKeysNullable<N, EK, EV>
{
private:
    MapState<EK, EV> *mapState;
    ValueState<EV> *nullState;

protected:
    MapState<EK, EV> *getMapState() override { return mapState; };
    ValueState<EV> *getNullState() override { return nullState; };

public:
    KeyedStateMapViewWithKeysNullable(MapState<EK, EV> *mapState, ValueState<EV> *nullState) : mapState(mapState), nullState(nullState) {};
    void setCurrentNamespace(N nameSpace) override { /* UNSUPPORTED*/ };
    void clear() override
    {
        getMapState()->clear();
        getNullState()->clear();
    };
};
template <typename N, typename EK, typename EV>
class StateMapViewWithKeysNotNull : public StateMapView<N, EK, EV>
{
private:
public:
    std::optional<EV> get(const std::optional<EK>& key) override {
        return getMapState()->get(*key);
    };
    void put(const std::optional<EK>& key, const EV& value) override { getMapState()->put(*key, value); };
    void remove(const std::optional<EK>& key) { getMapState()->remove(*key); };
    bool contains(const std::optional<EK>& key) { return getMapState()->contains(*key); };
    void clear() override { getMapState()->clear(); };

protected:
    virtual MapState<EK, EV> *getMapState() = 0;
};

template <typename N, typename EK, typename EV>
class KeyedStateMapViewWithKeysNotNull : public StateMapViewWithKeysNotNull<N, EK, EV>
{
private:
    MapState<EK, EV> *mapState;

public:
    KeyedStateMapViewWithKeysNotNull(MapState<EK, EV> *mapState) : mapState(mapState) {};
    void setCurrentNamespace(N nameSpace) override {
    std::runtime_error("Unsupported");}

protected:
    MapState<EK, EV> *getMapState() override { return mapState; };
};

#endif // FLINK_TNEL_STATEMAPVIEW_H
