/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#ifndef FLINK_TNEL_STREAMOPERATORSTATEHANDLER_H
#define FLINK_TNEL_STREAMOPERATORSTATEHANDLER_H

#include <string>
#include <iostream>
#include "runtime/state/AbstractKeyedStateBackend.h"
#include "StreamOperatorStateContext.h"
#include "runtime/state/DefaultKeyedStateStore.h"
#include "StreamTaskStateInitializerImpl.h"
#include "streaming/api/operators/OperatorSnapshotFutures.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/state/CheckpointStreamFactory.h"
#include "runtime/checkpoint/SavepointType.h"
#include "runtime/state/StateSnapshotContextSynchronousImpl.h"
#include "runtime/state/OperatorStateBackend.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/SnapshotStrategyRunner.h"


template<typename K>
class StreamOperatorStateHandler {
public:
    StreamOperatorStateHandler(StreamOperatorStateContextImpl<K> *context)
    {
        this->context = context;
        this->keyedStateBackend = context->keyedStateBackend();
        this->operatorStateBackend = context->operatorStateBackend();
        if (keyedStateBackend != nullptr) {
            keyedStateStore = new DefaultKeyedStateStore<K>(dynamic_cast<AbstractKeyedStateBackend<K> *>(keyedStateBackend));
        } else {
            keyedStateStore = nullptr;
        }
    };

    ~StreamOperatorStateHandler()
    {
        LOG("StreamOperatorStateHandler::~StreamOperatorStateHandler - delete keyedStateBackend");
        if (context != nullptr) {
            delete context;
            context = nullptr;
        }
        if (keyedStateStore != nullptr) {
            delete keyedStateStore;
            keyedStateStore = nullptr;
        }
    }
    void setCurrentKey(K key)
    {
        keyedStateBackend->setCurrentKey(key);
    };
    K getCurrentKey()
    {
        return keyedStateBackend->getCurrentKey();
    };
    AbstractKeyedStateBackend<K> *getKeyedStateBackend()
    {
        // Downcasting from CheckpointableKeyedStateBackend. This is temporarily OK because AbstractKeyedStateBackend
        // is currently the only direct inheritor of CheckpointableStateBackend
        return dynamic_cast<AbstractKeyedStateBackend<K> *>(keyedStateBackend);
    };
    DefaultKeyedStateStore<K> *getKeyedStateStore()
    {
        return keyedStateStore;
    };

    void dispose()
    {
        if (keyedStateBackend != nullptr) {
            keyedStateBackend->dispose();
        }
    }

    OperatorSnapshotFutures *snapshotState();

    void notifyCheckpointComplete(long checkpointId)
    {
        auto backend = dynamic_cast<RocksdbKeyedStateBackend<K>*>(keyedStateBackend);
        if (backend) {
            backend->notifyCheckpointComplete(checkpointId);
        }
    }

    class CheckpointedStreamOperator {
    public:
        virtual void snapshotState() {}
        virtual void initializeState() {}
    };

    OperatorSnapshotFutures *SnapshotState(
        CheckpointedStreamOperator *streamOperator,
        InternalTimeServiceManager<K> *timeServiceManager,
        std::string operatorName,
        long checkpointId,
        long timestamp,
        CheckpointOptions *checkpointOptions,
        CheckpointStreamFactory *checkpointStreamFactory,
        bool isUsingCustomRawKeyedState)
    {
        KeyGroupRange *keyGroupRange = KeyGroupRange::EMPTY_KEY_GROUP_RANGE();
        if (keyedStateBackend != nullptr) {
            keyGroupRange = keyedStateBackend->getKeyGroupRange();
        }

        auto snapshotInProgress = new OperatorSnapshotFutures();

        auto snapshotContext = new StateSnapshotContextSynchronousImpl(checkpointId,
            timestamp,
            checkpointStreamFactory,
            keyGroupRange);

        snapshotState(streamOperator,
            timeServiceManager,
            operatorName,
            checkpointId,
            timestamp,
            checkpointOptions,
            checkpointStreamFactory,
            snapshotInProgress,
            snapshotContext,
            isUsingCustomRawKeyedState);

        return snapshotInProgress;
    }

    void snapshotState(
        CheckpointedStreamOperator *streamOperator,
        InternalTimeServiceManager<K> *timeServiceManager,
        std::string operatorName,
        long checkpointId,
        long timestamp,
        CheckpointOptions *checkpointOptions,
        CheckpointStreamFactory *checkpointStreamFactory,
        OperatorSnapshotFutures *snapshotInProgress,
        StateSnapshotContextSynchronousImpl *snapshotContext,
        bool isUsingCustomRawKeyedState)
    {
        try {
            if (timeServiceManager != nullptr) {
                if (keyedStateBackend == nullptr) {
                    THROW_LOGIC_EXCEPTION("keyedStateBackend should be available with timeServiceManager");
                }

                AbstractKeyedStateBackend<K>* abstractBackend =
                    dynamic_cast<AbstractKeyedStateBackend<K>*>(keyedStateBackend);

                bool requiresLegacyRawKeyedStateSnapshots =
                    abstractBackend && abstractBackend->requiresLegacySynchronousTimerSnapshots(checkpointOptions->GetCheckpointType());
                if (requiresLegacyRawKeyedStateSnapshots) {
                    if (isUsingCustomRawKeyedState) {
                        THROW_LOGIC_EXCEPTION("Attempting to snapshot timers to raw keyed state, but this operator has custom raw keyed state to write.");
                    }

                    timeServiceManager->snapshotToRawKeyedState(snapshotContext->getRawKeyedOperatorStateOutput(), operatorName);
                }
            }
            
            streamOperator->snapshotState();
            snapshotInProgress->setKeyedStateRawFuture(snapshotContext->getKeyedStateStreamFuture());
            snapshotInProgress->setOperatorStateRawFuture(snapshotContext->getOperatorStateStreamFuture());

            if (operatorStateBackend) {
                snapshotInProgress->setOperatorStateManagedFuture(
                    operatorStateBackend->snapshot(checkpointId, timestamp, checkpointStreamFactory, checkpointOptions)
                );
            }

            if (keyedStateBackend) {
                if (isCanonicalSavepoint(checkpointOptions->GetCheckpointType())) {
                    // TTODO
                    // Create a snapshot runner with prepareCanonicalSavepoint()
                    // and set the snapshot as keyedStateManagedFuture
                } else {
                    snapshotInProgress->setKeyedStateManagedFuture(
                        keyedStateBackend->snapshot(checkpointId, timestamp, checkpointStreamFactory, checkpointOptions)
                    );
                }
            }
        } catch (...) {
            try {
                snapshotInProgress->cancel();
            } catch (...) {
                // Do nothing
            }
            std::string snapshotFailMessage = "Could not complete snapshot "
                            + std::to_string(checkpointId)
                            + " for operator "
                            + operatorName
                            + ".";
            
            try {
                snapshotContext->closeExceptionally();
            } catch (...) {
                // Do nothing
            }
            THROW_LOGIC_EXCEPTION(snapshotFailMessage);
        }
    };

private:
    // own backend
    StreamOperatorStateContextImpl<K> *context = nullptr;
    bool isCanonicalSavepoint(SnapshotType *snapshotType)
    {
        return snapshotType->IsSavepoint()
            && dynamic_cast<SavepointType *>(snapshotType)->getFormatType() == SavepointFormatType::CANONICAL;
    };

    // own backend
    CheckpointableKeyedStateBackend<K> *keyedStateBackend;
    DefaultKeyedStateStore<K> *keyedStateStore;
    OperatorStateBackend *operatorStateBackend;
};

#endif // FLINK_TNEL_STREAMOPERATORSTATEHANDLER_H