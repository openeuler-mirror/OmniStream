#ifndef FLINK_TNEL_STATEINITIALIZATIONCONTEXTIMPL_H
#define FLINK_TNEL_STATEINITIALIZATIONCONTEXTIMPL_H

#include <memory>
#include <optional>
#include <stdexcept>
#include "runtime/state/OperatorStateBackend.h"
#include "runtime/state/DefaultKeyedStateStore.h"
// Forward declarations of dependencies
#include "runtime/state/KeyGroupStatePartitionStreamProvider.h"
#include "core/utils/Iterator.h"

using omnistream::utils::Iterable;
using omnistream::utils::Iterator;

/** 
 * StateInitializationContextImpl 类整合了所有状态初始化相关的功能
 * 它合并了原来的 ManagedInitializationContext、FunctionInitializationContext 和 StateInitializationContext 接口的功能
 */
template <typename K>
class StateInitializationContextImpl {
private:
    /** Signal whether any state to restore was found */
    std::optional<uint64_t> restoredCheckpointId;
    
    OperatorStateBackend *operatorStateBackend;
    
    DefaultKeyedStateStore<K> *keyedStateStore;
    
    std::shared_ptr<Iterable<std::shared_ptr<KeyGroupStatePartitionStreamProvider>>> rawKeyedStateInputs;
    
    std::shared_ptr<Iterable<std::shared_ptr<StatePartitionStreamProvider>>> rawOperatorStateInputs;
    
public:
    /**
     * 构造函数
     * @param restoredCheckpointId 恢复的检查点ID
     * @param operatorStateBackend 操作符状态后端
     * @param keyedStateStore 键控状态存储
     * @param rawKeyedStateInputs 键控状态输入流的可迭代对象
     * @param rawOperatorStateInputs 操作符状态输入流的可迭代对象
     */
    StateInitializationContextImpl(
        std::optional<uint64_t> restoredCheckpointId,
        OperatorStateBackend *operatorStateBackend,
        DefaultKeyedStateStore<K> *keyedStateStore,
        std::shared_ptr<Iterable<std::shared_ptr<KeyGroupStatePartitionStreamProvider>>> rawKeyedStateInputs = nullptr,
        std::shared_ptr<Iterable<std::shared_ptr<StatePartitionStreamProvider>>> rawOperatorStateInputs = nullptr)
        : restoredCheckpointId(restoredCheckpointId),
          operatorStateBackend(operatorStateBackend),
          keyedStateStore(keyedStateStore),
          rawKeyedStateInputs(rawKeyedStateInputs),
          rawOperatorStateInputs(rawOperatorStateInputs) {
        // No additional initialization needed
    }
    
    /** 
     * Returns true, if state was restored from the snapshot of a previous execution.
     * 如果状态是从先前执行的快照中恢复的，则返回true。
     */
    bool isRestored() const {
        return restoredCheckpointId.has_value();
    }
    
    /**
     * Returns id of the restored checkpoint, if state was restored from the snapshot of a previous
     * execution.
     * 如果状态是从先前执行的快照中恢复的，则返回恢复的检查点的ID。
     */
    std::optional<uint64_t> getRestoredCheckpointId() const {
        return restoredCheckpointId;
    }
    
    /**
     * Returns an iterable to obtain input streams for previously stored operator state partitions that
     * are assigned to this operator.
     * @param[out] iterable Output parameter to store the iterable pointer
     */
    std::shared_ptr<Iterable<std::shared_ptr<StatePartitionStreamProvider>>> getRawOperatorStateInputs() const {
        return rawOperatorStateInputs;
    }
    
    /**
     * Returns an iterable to obtain input streams for previously stored keyed state partitions that
     * are assigned to this operator.
     * @param[out] iterable Output parameter to store the iterable pointer
     */
    std::shared_ptr<Iterable<std::shared_ptr<KeyGroupStatePartitionStreamProvider>>> getRawKeyedStateInputs() const {
        return rawKeyedStateInputs;
    }
    
    /**
     * Returns an interface that allows for registering operator state with the backend.
     * 返回一个允许向后端注册操作符状态的接口。
     */
    OperatorStateBackend *getOperatorStateBackend() const {
        return operatorStateBackend;
    }
    
    /**
     * Returns an interface that allows for registering keyed state with the backend.
     * 返回一个允许向后端注册键控状态的接口。
     */
    DefaultKeyedStateStore<K> *getKeyedStateStore() const {
        return keyedStateStore;
    }   
};


#endif // FLINK_TNEL_STATEINITIALIZATIONCONTEXTIMPL_H
