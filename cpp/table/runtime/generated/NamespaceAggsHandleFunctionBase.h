#ifndef NAMESPACEAGGSHANDLEFUNCTIONBASE_H
#define NAMESPACEAGGSHANDLEFUNCTIONBASE_H

#include "table/runtime/dataview/StateDataViewStore.h"
#include "table/data/RowData.h"

template <typename N>
class NamespaceAggsHandleFunctionBase {
public:
    virtual ~NamespaceAggsHandleFunctionBase() = default;

    // 初始化方法
    virtual void open(StateDataViewStore* store) = 0;

    // 设置当前累加器
    virtual void setAccumulators(N namespace_val, RowData* accumulators) = 0;

    // 累积输入值
    virtual void accumulate(RowData* input_row) = 0;

    // 回撤输入值
    virtual void retract(RowData* input_row) = 0;

    // 合并其他累加器
    virtual void merge(N namespace_val, RowData* other_acc) = 0;

    // 创建初始累加器
    virtual RowData *createAccumulators(int accumulatorArity) = 0;

    // 获取当前累加器
    virtual RowData *getAccumulators() = 0;

    // 清理命名空间
    virtual void Cleanup(N namespace_val) = 0;

    // 关闭资源
    virtual void close() = 0;

};

#endif //NAMESPACEAGGSHANDLEFUNCTIONBASE_H