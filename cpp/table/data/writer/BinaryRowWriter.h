#ifndef FLINK_TNEL_BINARYROWWRITER_H
#define FLINK_TNEL_BINARYROWWRITER_H


#include "AbstractBinaryWriter.h"
#include "table/data/binary/BinaryRowData.h"

class BinaryRowWriter : public AbstractBinaryWriter{
public:
    explicit BinaryRowWriter(BinaryRowData *row);

    BinaryRowWriter(BinaryRowData *row,  int initialSize);
    ~BinaryRowWriter() override = default;

    // virtual
    void writeLong(int pos, long value) override;
    void writeInt(int pos, int value) override;

    void reset() override;

    void setNullAt(int pos) override;

    void complete() override;

protected:
    int getFieldOffset(int pos) override;

protected:
    void setNullBit(int ordinal) override;

public:

    // non-virtual
    void writeRowKind(RowKind kind);


private:

    int nullBitsSizeInBytes_{};
    BinaryRowData * row_{};
    int fixedSize_{};

};


#endif //FLINK_TNEL_BINARYROWWRITER_H
