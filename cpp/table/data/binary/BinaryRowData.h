#pragma once


#include <bitset>
#include "../RowData.h"
#include "../TimestampData.h"
#include "BinarySection.h"
#include "TypedSetters.h"
#include "BinarySegmentUtils.h"

class BinaryRowData : public RowData, public BinarySection, public TypedSetters {

public:
    explicit BinaryRowData(int arity);
    explicit BinaryRowData(int arity, int length);
    // ~BinaryRowData() {
    //     if (memoryBuffer != nullptr) {
    //         delete[] memoryBuffer;
    //     }
    // }
    virtual  ~BinaryRowData() override;

    //virtual
    int getArity() override;

    void setRowKind(RowKind kind) override;

    bool isNullAt(int pos) override;

    RowKind getRowKind() override;

    // Getter and setter
    long* getLong(int pos) override;
    void setLong(int pos, long value) override;
    void setLong(int pos, long* value);

    bool *getBool(int pos) override;
    void setBool(int pos, bool value) override;
    void setBool(int pos, bool *value);

    int *getInt(int pos) override;
    void setInt(int pos, int value) override;

    TimestampData* getTimestamp(int pos) override;
    TimestampData* getTimestampPrecise(int pos) override;
    void setTimestamp(int pos, TimestampData& value, int precision) override;

    BinaryStringData* getString(int pos) override;
    void setString(int pos, BinaryStringData* value) override;

    std::string_view getStringView(int pos) override;
    void setStringView(int pos, std::string_view value) override;

    void setNullAt(int pos);

    // Utitilities for `getString`
    void writeFixLenVarchar(int fieldOffset, const uint8_t* bytes, int len);
    void writeVarLenVarchar(int fieldOffset, const uint8_t* bytes, int len);
    int getNumberOfBytesToNearestWord(int numBytes);
    void zeroOutPaddingBytes(int fieldOffset, int numBytes);
    void setOffsetAndSize(int headerOffset, int varcharOffset, int len);

    //non virtual
    int getFixedLengthPartSize() const;
    void setSizeInBytes(int sizeInBytes);

    bool operator==(const RowData &other) const override;

    int hashCode() const override;
    [[nodiscard]] int hashCodeFast() const override;

    //static
    static int calculateBitSetWidthInBytes(int arity);
    static const int HEADER_SIZE_IN_BITS = 8;
    static int calculateFixPartSizeInBytes(int arity);

    // assume all field are fixed length
    static BinaryRowData * createBinaryRowDataWithMem(int arity);
    static BinaryRowData * createRowFromSubJoinedRows(BinaryRowData * row1, BinaryRowData * row2 );
    // for debug
    void printSegInBinary() const;

    int getNullBitsSizeInBytes() const {
        return nullBitsSizeInBytes_;
    }

    RowData* copy() override;
   

private:
    int arity_;
    std::vector<uint8_t> types; // type 1 => long or FixLenVarchar, type 2 => VarLenVarchar
    int nullBitsSizeInBytes_;
    void setNotNullAt(int i);
    int getFieldOffset(int pos);

};

namespace std
{
    template<>
    struct hash<BinaryRowData>
    {
        std::size_t operator()(const BinaryRowData& binaryRowData) const noexcept
        {
            return binaryRowData.hashCode();
        }
    };
    
}

