#ifndef OMNISTREAM_KEYSELECTOR_H
#define OMNISTREAM_KEYSELECTOR_H

#include "functions/KeySelect.h"
#include "functions/MapFunction.h"
#include "basictypes/Object.h"
#include "basictypes/Tuple2.h"

class MockKeyedBy : public KeySelect<Object> {
public:
    Object* getKey(Object* value) override {
        auto *tuple = reinterpret_cast<Tuple2 *>(value);
        return tuple->f0;
    }
};


#endif //OMNISTREAM_KEYSELECTOR_H
