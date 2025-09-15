/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_MUTIPLEINPUTSELECTIONHANDLER_H
#define OMNISTREAM_MUTIPLEINPUTSELECTIONHANDLER_H

#include <stdexcept>
#include "io/DataInputStatus.h"

namespace omnistream {

    class MutipleInputSelectionHandler {
    public:
    explicit MutipleInputSelectionHandler(int inputCount)
        : dataFinishedButNotPartition(0), operatingMode(NO_INPUT_SELECTABLE) {
        allSelectedMask = (1L << inputCount) - 1;
        availableInputsMask = allSelectedMask;
        notFinishedInputsMask = allSelectedMask;
    }

    DataInputStatus updateStatusAndSelection(DataInputStatus inputStatus, int inputIndex)
    {
                switch (inputStatus) {
                    case DataInputStatus::MORE_AVAILABLE:
                        nextSelection();
                        checkBitMask(availableInputsMask, inputIndex);
                        return DataInputStatus::MORE_AVAILABLE;
                    case DataInputStatus::NOTHING_AVAILABLE:
                        availableInputsMask = unsetBitMask(availableInputsMask, inputIndex);
                        break;
                    case DataInputStatus::END_OF_DATA:
                        dataFinishedButNotPartition = setBitMask(dataFinishedButNotPartition, inputIndex);
                        updateModeOnEndOfData();
                        break;
                    case DataInputStatus::END_OF_INPUT:
                        dataFinishedButNotPartition = unsetBitMask(dataFinishedButNotPartition, inputIndex);
                        notFinishedInputsMask = unsetBitMask(notFinishedInputsMask, inputIndex);
                        break;
                    default:
                        throw std::runtime_error("Unsupported inputStatus");
                }
                nextSelection();
                return calculateOverallStatus(inputStatus);
        }

        void updateModeOnEndOfData()
        {
            bool allDataInputsFinished =
                    ((dataFinishedButNotPartition | ~notFinishedInputsMask) & allSelectedMask)
                    == allSelectedMask;
            if (allDataInputsFinished) {
                operatingMode = OperatingMode::ALL_DATA_INPUTS_FINISHED;
            } else if (operatingMode
                       == OperatingMode::INPUT_SELECTABLE_PRESENT_NO_DATA_INPUTS_FINISHED) {
                operatingMode = OperatingMode::INPUT_SELECTABLE_PRESENT_SOME_DATA_INPUTS_FINISHED;
            }
        }

    DataInputStatus calculateOverallStatus(DataInputStatus updatedStatus)
    {
                if (areAllInputsFinished()) {
                    return DataInputStatus::END_OF_INPUT;
                }

                if (updatedStatus == DataInputStatus::END_OF_DATA
                && operatingMode == OperatingMode::ALL_DATA_INPUTS_FINISHED) {
                    return DataInputStatus::END_OF_DATA;
                }

                if (isAnyInputAvailable()) {
                    return DataInputStatus::MORE_AVAILABLE;
                } else {
                    long selectedNotFinishedInputMask = selectedInputsMask & notFinishedInputsMask;
                    if (selectedNotFinishedInputMask == 0) {
                        throw std::runtime_error(
                            "Can not make a progress: all selected inputs are already finished");
                    }
                    return DataInputStatus::NOTHING_AVAILABLE;
                }
        }

        inline bool areAllInputsFinished()
        {
            return notFinishedInputsMask == 0;
        }

        inline bool isAnyInputAvailable() const
        {
            return (selectedInputsMask & availableInputsMask & notFinishedInputsMask) != 0;
        }

        void nextSelection()
        {
            switch (operatingMode) {
                case NO_INPUT_SELECTABLE:
                case ALL_DATA_INPUTS_FINISHED:
                    selectedInputsMask = -1;
                    break;
                case INPUT_SELECTABLE_PRESENT_NO_DATA_INPUTS_FINISHED:
                    break;
                case INPUT_SELECTABLE_PRESENT_SOME_DATA_INPUTS_FINISHED:
                    break;
            }
        }

        void SetAvailableInput(int inputIndex)
        {
            availableInputsMask = setBitMask(availableInputsMask, inputIndex);
        }

        bool ShouldSetAvailableForAnotherInput()
        {
            return (selectedInputsMask & allSelectedMask & ~availableInputsMask) != 0;
        }

        inline long setBitMask(long mask, int inputIndex)
        {
            return mask | 1L << inputIndex;
        }

        inline long unsetBitMask(long mask, int inputIndex)
        {
            return mask & ~(1L << inputIndex);
        }

        inline bool checkBitMask(long mask, int inputIndex)
        {
            return (mask & (1L << inputIndex)) != 0;
        }

        int selectNextInputIndex(int lastReadInputIndex);

    private:
     long selectedInputsMask = -1;

     long allSelectedMask;

     long availableInputsMask;

     long notFinishedInputsMask;

     long dataFinishedButNotPartition;

     enum OperatingMode {
         NO_INPUT_SELECTABLE,
         INPUT_SELECTABLE_PRESENT_NO_DATA_INPUTS_FINISHED,
         INPUT_SELECTABLE_PRESENT_SOME_DATA_INPUTS_FINISHED,
         ALL_DATA_INPUTS_FINISHED
     };

     OperatingMode operatingMode;
    };

} // omnistream

#endif //OMNISTREAM_MUTIPLEINPUTSELECTIONHANDLER_H
