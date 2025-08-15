#include "JoinedRowData.h"
#include <functional>
#include <stdexcept>

JoinedRowData::JoinedRowData()
    : RowData(RowData::JoinedRowDataID), rowKind(RowKind::INSERT), row1(nullptr), row2(nullptr) {}

JoinedRowData::JoinedRowData(RowData* row1_, RowData* row2_)
    : RowData(RowData::JoinedRowDataID), rowKind(RowKind::INSERT), row1(row1_), row2(row2_) {}

JoinedRowData::JoinedRowData(RowKind rowKind_, RowData* row1_, RowData* row2_)
    : RowData(RowData::JoinedRowDataID), rowKind(rowKind_), row1(row1_), row2(row2_) {}

JoinedRowData* JoinedRowData::replace(RowData* row1_, RowData* row2_) {
    this->row1 = row1_;
    this->row2 = row2_;
    return this;
}

int JoinedRowData::getArity() {
    return row1->getArity() + row2->getArity();
}

RowKind JoinedRowData::getRowKind() {
    return rowKind;
}

void JoinedRowData::setRowKind(RowKind kind) {
    rowKind = kind;
}

bool JoinedRowData::isNullAt(int pos) {
    if (pos < row1->getArity()) {
        return row1->isNullAt(pos);
    } else {
        return row2->isNullAt(pos - row1->getArity());
    }
}

long* JoinedRowData::getLong(int pos) {
    if (pos < row1->getArity()) {
        return row1->getLong(pos);
    } else {
        return row2->getLong(pos - row1->getArity());
    }
}

bool *JoinedRowData::getBool(int pos)
{
    if (pos < row1->getArity()) {
        return row1->getBool(pos);
    } else {
        return row2->getBool(pos - row1->getArity());
    }
}

RowData *JoinedRowData::getRow1()
{
    return row1;
}
 
RowData *JoinedRowData::getRow2()
{
    return row2;
}

TimestampData *JoinedRowData::getTimestamp(int pos) {
    if (pos < row1->getArity()) {
        return row1->getTimestamp(pos);
    } else {
        return row2->getTimestamp(pos - row1->getArity());
    }
}

TimestampData *JoinedRowData::getTimestampPrecise(int pos) {
    if (pos < row1->getArity()) {
        return row1->getTimestampPrecise(pos);
    } else {
        return row2->getTimestampPrecise(pos - row1->getArity());
    }
}

int *JoinedRowData::getInt(int pos) {
    if (pos < row1->getArity()) {
        return row1->getInt(pos);
    } else {
        return row2->getInt(pos - row1->getArity());
    }
}

int JoinedRowData::hashCode() const {
    return hash_combine(row1->hashCode(), row2->hashCode());
}

bool JoinedRowData::operator==(const RowData &other) const {
    // Check for self-reference
    if (this == &other) {
        return true;
    }
    
    auto castedOther = dynamic_cast<const JoinedRowData*>(&other);
    if (castedOther == nullptr) {
        // If it is not compared to a BinaryRowData return false directly
        return false;
    }
    
    return this->row1 == castedOther->row1 && this->row2 == castedOther->row2;
}

void JoinedRowData::setLong(int pos, long value) {
    if (pos < row1->getArity()) {
        row1->setLong(pos, value);
    } else {
        row2->setLong(pos - row1->getArity(), value);
    }
}

void JoinedRowData::setInt(int pos, int value) {
    if (pos < row1->getArity()) {
        row1->setInt(pos, value);
    } else {
        row2->setInt(pos - row1->getArity(), value);
    }
}

void JoinedRowData::setTimestamp(int pos, TimestampData &value, int precision) {
    if (pos < row1->getArity()) {
        row1->setTimestamp(pos, value, precision);
    } else {
        row2->setTimestamp(pos- row1->getArity(), value, precision);
    }
}

void JoinedRowData::setString(int pos, BinaryStringData *value) {
    if (pos < row1->getArity()) {
        row1->setString(pos, value);
    } else {
        row2->setString(pos - row1->getArity(), value);
    }
}

int JoinedRowData::hashCodeFast() const {
    return hash_combine(row1->hashCodeFast(), row2->hashCodeFast());
}

std::string_view JoinedRowData::getStringView(int pos)
{
    if (pos < row1->getArity()) {
        return row1->getStringView(pos);
    } else {
        return row2->getStringView(pos - row1->getArity());
    }
}

void JoinedRowData::setStringView(int pos, std::string_view value)
{
    if (pos < row1->getArity()) {
        row1->setStringView(pos, value);
    } else {
        row2->setStringView(pos - row1->getArity(), value);
    }
}
