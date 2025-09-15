#include "FieldGetter.h"

FieldGetter::FieldGetter(int fieldPos, getFieldByPosFn getFieldByPos) :
fieldPos_(fieldPos),getFieldByPos_(getFieldByPos) {}

void *FieldGetter::getFieldOrNull(RowData *row) {
    return (row->*getFieldByPos_)(fieldPos_);
}
