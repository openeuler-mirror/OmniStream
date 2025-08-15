//
// Created by root on 9/17/24.
//

#ifndef FLINK_TNEL_TYPEDSETTERS_H
#define FLINK_TNEL_TYPEDSETTERS_H


class TypedSetters {
public:

    virtual  void setLong(int pos, long value) = 0;
/**
    void setNullAt(int pos);

    void setBoolean(int pos, boolean value);

    void setByte(int pos, byte value);

    void setShort(int pos, short value);

    void setInt(int pos, int value);

    void setLong(int pos, long value);

    void setFloat(int pos, float value);

    void setDouble(int pos, double value);

    /-**
     * Set the decimal column value.
     *
     * <p>Note: Precision is compact: can call {@link #setNullAt} when decimal is null. Precision is
     * not compact: can not call {@link #setNullAt} when decimal is null, must call {@code
     * setDecimal(pos, null, precision)} because we need update var-length-part.
     *-/
    void setDecimal(int pos, DecimalData value, int precision);

    /-**
     * Set Timestamp value.
     *
     * <p>Note: If precision is compact: can call {@link #setNullAt} when TimestampData value is
     * null. Otherwise: can not call {@link #setNullAt} when TimestampData value is null, must call
     * {@code setTimestamp(pos, null, precision)} because we need to update var-length-part.
     *-/
    void setTimestamp(int pos, TimestampData value, int precision);

    **/

};


#endif //FLINK_TNEL_TYPEDSETTERS_H
