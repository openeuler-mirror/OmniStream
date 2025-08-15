#ifndef FLINK_TNEL_STATEDATAVIEW_H
#define FLINK_TNEL_STATEDATAVIEW_H
#include "DataView.h"

template <typename N>
class StateDataView : public DataView
{
public:
    virtual void setCurrentNamespace(N nameSpace) = 0;
};

#endif // FLINK_TNEL_STATEDATAVIEW_H
