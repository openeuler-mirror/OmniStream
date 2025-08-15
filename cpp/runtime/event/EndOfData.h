/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
*/

#ifndef ENDOFDATA_H
#define ENDOFDATA_H

#include "RuntimeEvent.h"
#include "io/network/api/StopMode.h"

namespace omnistream {
    class EndOfData : public RuntimeEvent {
    public:
        explicit EndOfData(StopMode mode) : mode_(mode) {}
        StopMode getStopMode() const { return mode_; }

        int GetEventClassID() override
        {
            return AbstractEvent::endOfData;
        }

        std::string GetEventClassName() override
        {
            return "EndOfData";
        }
        // Comparison and hash
        bool Equals(const RuntimeEvent* other) const
        {
            if (this == other) {
                return true;
            }
            auto* eod = dynamic_cast<const EndOfData*>(other);
            return eod != nullptr && mode_ == eod->mode_;
        }

        size_t hashCode() const
        {
            return std::hash<StopMode>{}(mode_);
        }

        std::string toString() const
        {
            return "EndOfData{mode=" + std::to_string(static_cast<int>(mode_)) + "}";
        }
        // Optionally add operator== if direct comparisons are needed
        bool operator==(const EndOfData& rhs) const
        {
            return mode_ == rhs.mode_;
        }
    private:
        StopMode mode_;
    };
}

#endif // ENDOFDATA_H
