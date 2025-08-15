#ifndef FLINK_TNEL_JOINEDROWDATA_H
#define FLINK_TNEL_JOINEDROWDATA_H

#include "table/data/RowData.h"
#include "table/data/TimestampData.h"
#include "table/RowKind.h"

class JoinedRowData : public RowData {
public:
    JoinedRowData();
    JoinedRowData(RowData* row1_, RowData* row2_);
    JoinedRowData(RowKind rowKind_, RowData* row1_, RowData* row2_);

    ~JoinedRowData() override = default;

    JoinedRowData* replace(RowData* row1_, RowData* row2_);

    int getArity() override;
    RowKind getRowKind() override;
    void setRowKind(RowKind kind) override;
    bool isNullAt(int pos) override;
    long *getLong(int pos) override;
    bool *getBool(int pos) override;
    int * getInt(int pos) override;
    TimestampData *getTimestamp(int pos) override;
    TimestampData *getTimestampPrecise(int pos) override;
    std::string_view getStringView(int pos);
    void setStringView(int pos, std::string_view value);

    RowData * getRow1();
    RowData * getRow2();

    void setLong(int pos, long value) override;

    void setInt(int pos, int value) override;

    void setTimestamp(int pos, TimestampData& value, int precision) override;

    void setString(int pos, BinaryStringData* value) override;

    int hashCode() const override;
    [[nodiscard]] int hashCodeFast() const override;
    bool operator==(const RowData &other) const override;
    // Not needed for q4:
    // bool getBoolean(int pos);
    // uint8_t getByte(int pos);
    // short getShort(int pos) override;
    // int getInt(int pos) override;
    // float getFloat(int pos) override;
    // double getDouble(int pos) override;
    // StringData getString(int pos) override;
    // DecimalData getDecimal(int pos, int precision, int scale) override; // Need DecimalData
    // template <typename T>
    // RawValueData<T> getRawValue(int pos) override;
    // std::vector<uint8_t> getBinary(int pos) override;
    // ArrayData getArray(int pos) override;  // Need ArrayData
    // MapData getMap(int pos) override;  // Need MapData
    // RowData getRow(int pos, int numFields) override; // Need RowData
    // bool equals(const JoinedRowData& other) const;
    // int hashCode() const;
    // std::string toString() const;

private:
    RowKind rowKind;
    RowData* row1;
    RowData* row2;
};

#endif //FLINK_TNEL_JOINEDROWDATA_H
