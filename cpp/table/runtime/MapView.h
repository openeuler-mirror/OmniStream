#ifndef FLINK_TNEL_MAPVIEW_H
#define FLINK_TNEL_MAPVIEW_H
#include "DataView.h"
#include <emhash7.hpp>
#include <optional>

template <typename K, typename V>
class MapView : public DataView
{
private:
    emhash7::HashMap<K, V> *map;

public:
    MapView() {map = new emhash7::HashMap<K, V>();};
    virtual emhash7::HashMap<K, V> *getMap() { return map; }
    void setMap(emhash7::HashMap<K, V> *map) { this->map = map; };

    virtual std::optional<V> get(const std::optional<K>& key)
    {
        auto it = map->find(*key);
        if(it == map->end()) {
            return std::nullopt;
        } else {
            return it->second;
        }
    };
    virtual void put(const std::optional<K>& key, const V& value)
    {
        map->emplace(*key, value);
    }
    ~MapView() {
        delete map;
    }
};

#endif // FLINK_TNEL_MAPVIEW_H
