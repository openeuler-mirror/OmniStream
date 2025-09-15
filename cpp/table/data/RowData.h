#ifndef FLINK_TNEL_ROWDATA_H
#define FLINK_TNEL_ROWDATA_H

#include "table/RowKind.h"
#include "../types/logical/LogicalType.h"
#include "FieldGetter.h"
#include "TimestampData.h"
#include "binary/BinaryStringData.h"
#include "table/data/binary/MurmurHashUtils.h"
#include <functional>
#include <cstddef>

inline std::size_t hash_combine(std::size_t lhs, std::size_t rhs) {
    lhs ^= rhs + 0x9e3779b9 + (lhs << 6) + (lhs >> 2);
    return lhs;
}
class RowData {

public:
    explicit RowData(int rowDataTypeId);

    virtual ~RowData() = default;

    //virutal
    /**
   * Returns the number of fields in this row.
   *
   * <p>The number does not include {@link RowKind}. It is kept separately.
   */
    virtual int getArity() = 0;

    /**
     * Sets the kind of change that this row describes in a changelog.
     *
     * @see RowKind
     */
    virtual void setRowKind(RowKind kind) = 0;

    /** Returns true if the field is null at the given position. */
    virtual bool isNullAt(int pos) = 0;

    virtual long * getLong(int pos) = 0;

    virtual bool *getBool(int pos) = 0;

    virtual int * getInt(int pos) = 0;

    virtual BinaryStringData* getString(int pos);

    virtual RowKind getRowKind() = 0;

    virtual TimestampData* getTimestamp(int pos) = 0;

    virtual TimestampData* getTimestampPrecise(int pos) = 0;

    virtual void setLong(int pos, long value) { NOT_IMPL_EXCEPTION; };

    virtual void setBool(int pos, bool value) { NOT_IMPL_EXCEPTION; };

    virtual void setInt(int pos, int value) { NOT_IMPL_EXCEPTION; };

    virtual void setTimestamp(int pos, TimestampData& value, int precision) { NOT_IMPL_EXCEPTION; };

    virtual void setString(int pos, BinaryStringData* value) { NOT_IMPL_EXCEPTION; };

    virtual std::string_view getStringView(int pos)
    {
        NOT_IMPL_EXCEPTION;
    };
    virtual void setStringView(int pos, std::string_view value)
    {
        NOT_IMPL_EXCEPTION;
    };

    virtual bool operator==(const RowData &other) const = 0;
    virtual int hashCode() const = 0;
    virtual int hashCodeFast() const = 0;
    //constant
    static const int BinaryRowDataID = 0;
    static const int GenericRowDataID = 1;
    static const int JoinedRowDataID = 2;

    //non virtual
    [[nodiscard]] int getRowDataTypeId() const;

    static FieldGetter* createFieldGetter(LogicalType *fieldType, int fieldPos);

    void printRow();

    virtual RowData* copy() { return nullptr;};
private:
    int rowDataTypeID_ {-1};

};

namespace std
{
    template <>
    struct hash<RowData>
    {
        std::size_t operator()(const RowData &ns) const noexcept
        {
            return ns.hashCodeFast();
        }
    };
    template <>
    struct equal_to<RowData>
    {
        bool operator()(const RowData &lhs, const RowData &rhs) const noexcept
        {
            return lhs == rhs;
        }
    };
    //Attention: Be very careful when using this! It does not hash its address
    template <>
    struct hash<RowData*>
    {
        std::size_t operator()(const RowData* nsPtr) const noexcept
        {
            // std::cout<<"RowData::hash "<<nsPtr->hashCode()<<std::endl;
            return nsPtr->hashCodeFast();
        }
    };
    //Attention: Be very careful when using this! It does not compare the address. but the content
    template <>
    struct equal_to<RowData*>
    {
        bool operator()(const RowData* lhs, const RowData* rhs) const noexcept
        {
            // std::cout<<"RowData::Equal? "<<(*lhs == *rhs)<<std::endl;
            return *lhs == *rhs;
        }
    };
}

#endif //FLINK_TNEL_ROWDATA_H

