/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_BULK_WRITER_H
#define OMNISTREAM_BULK_WRITER_H

template <typename T>
class BulkWriter {
public:
    virtual ~BulkWriter() = default;
    virtual void addElement(T element) = 0;
    virtual void flush() = 0;
    virtual void finish() = 0;
    virtual void create() = 0;
};

#endif // OMNISTREAM_BULK_WRITER_H