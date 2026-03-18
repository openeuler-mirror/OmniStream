//
// Created by h30059066 on 2026/2/9.
//

#ifndef OMNISTREAM_SUBTASKCONNECTIONDESCRIPTOR_H
#define OMNISTREAM_SUBTASKCONNECTIONDESCRIPTOR_H

#include "RuntimeEvent.h"
#include "sstream"

namespace omnistream{
    class SubtaskConnectionDescriptor : public RuntimeEvent{
    public:
        SubtaskConnectionDescriptor(int inputIndex, int outputIndex) : inputSubtaskIndex(inputIndex),
                                                                       outputSubtaskIndex(outputIndex) {}
        bool operator==(const SubtaskConnectionDescriptor& other) const
        {
            return inputSubtaskIndex == other.inputSubtaskIndex && outputSubtaskIndex == other.outputSubtaskIndex;
        }

        std::string toString() {
            std::stringstream ss;
            ss << "SubtaskConnectionDescriptor{ inputSubtaskIndex=" << inputSubtaskIndex << ", outputSubtaskIndex="
               << outputSubtaskIndex << "}";
            return ss.str();
        }

        std::size_t hashCode() const {
            constexpr std::size_t kGoldenRatio = 0x9e3779b9;  // 2^32 / φ (φ ≈ 1.618)

            std::size_t seed = 0;

            std::size_t h1 = std::hash<int>{}(inputSubtaskIndex);
            seed ^= h1 + kGoldenRatio + (seed << 6) + (seed >> 2);

            std::size_t h2 = std::hash<int>{}(outputSubtaskIndex);
            seed ^= h2 + kGoldenRatio + (seed << 6) + (seed >> 2);

            return seed;
        }

    private:
        int inputSubtaskIndex;
        int outputSubtaskIndex;
    };
}

#endif //OMNISTREAM_SUBTASKCONNECTIONDESCRIPTOR_H