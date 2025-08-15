/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */

#ifndef OMNISTREAM_DUMMYSTREAMPARTITIONER_H

#define OMNISTREAM_DUMMYSTREAMPARTITIONER_H

#include "writer/RecordWriter.h"
#include "io/RecordWriterOutput.h"

using namespace omnistream;

class DummyStreamPartitioner : public datastream::StreamPartitioner<IOReadableWritable> {
public:
    bool isPointWise() const override
    {
        return false;
    }

    void setup(int numberOfChannels) override {}

    int selectChannel(IOReadableWritable *record) override
    {
        return 0;
    }

//    std::vector<int> selectChannelForBatch(IOReadableWritable &record)
//    {
//        return std::vector<int>();
//    }

    bool isBroadcast() const override
    {
        return false;
    }

    virtual std::unique_ptr<StreamPartitioner<IOReadableWritable>> copy() {
        return nullptr;
    };

    [[nodiscard]] virtual std::string toString() const{
        return {};
    }
};


#endif // OMNISTREAM_DUMMYSTREAMPARTITIONER_H
