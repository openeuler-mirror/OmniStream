/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef TASKMANAGERSERVICECONFIGURATION_H
#define TASKMANAGERSERVICECONFIGURATION_H


namespace omnistream {
    class TaskManagerServiceConfiguration {
    public:
            explicit TaskManagerServiceConfiguration(int num_threads)
                : numThreads_(num_threads) {
            }

            [[nodiscard]] int numbertOfThreads() const {
                return numThreads_;
            }

    private:
        int numThreads_;


    };
}



#endif //TASKMANAGERSERVICECONFIGURATION_H
