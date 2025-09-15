#ifndef FLINK_TNEL_FIELDGETTER_H
#define FLINK_TNEL_FIELDGETTER_H

//forward class declaration

class RowData;

typedef void * (RowData::*getFieldByPosFn) (int pos);

// TBD is FiledGetter necessary?
class FieldGetter {
public:
    FieldGetter(int fieldPos, getFieldByPosFn getFieldByPos);

    void * getFieldOrNull(RowData* row);

private:
    int fieldPos_;
    getFieldByPosFn getFieldByPos_;


};


#endif //FLINK_TNEL_FIELDGETTER_H
