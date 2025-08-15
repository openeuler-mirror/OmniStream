#ifndef NAMESPACEAGGSHANDLEFUNCTION_H
#define NAMESPACEAGGSHANDLEFUNCTION_H

#include "NamespaceAggsHandleFunctionBase.h"

template<typename N>
class NamespaceAggsHandleFunction : public NamespaceAggsHandleFunctionBase<N> {
public:
    virtual RowData *getValue(N namespaceVal) = 0;

    ~NamespaceAggsHandleFunction() override = default;
};

#endif //NAMESPACEAGGSHANDLEFUNCTION_H
