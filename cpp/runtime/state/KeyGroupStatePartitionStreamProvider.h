#pragma once

#include <memory>
#include <stdexcept>
#include <string>
#include <iostream>
#include <algorithm>  // For std::min

// Precondition checking macro
#define CHECK_NOT_NULL(ptr, message) \
    if (!(ptr)) { \
        throw std::invalid_argument(message); \
    }

/**
 * This class provides access to input streams that contain data of one state partition of a
 * partitionable state.
 */
class StatePartitionStreamProvider {
private:
    /** A ready-made stream that contains data for one state partition */
    std::shared_ptr<FSDataInputStream> stream;
    std::exception_ptr creationException;
    
public:
    /**
     * Creates a new StatePartitionStreamProvider that wraps a creation exception.
     */
    StatePartitionStreamProvider(std::exception_ptr creationException)
        : stream(nullptr),
          creationException(creationException) {
        CHECK_NOT_NULL(creationException, "Creation exception cannot be null");
    }
    
    /**
     * Creates a new StatePartitionStreamProvider that wraps an input stream.
     */
    StatePartitionStreamProvider(std::shared_ptr<FSDataInputStream> stream) {
        CHECK_NOT_NULL(stream, "Stream cannot be null");
        this->stream = stream;
        this->creationException = nullptr;
    }
    
    /**
     * Returns a stream with the data of one state partition.
     */
    virtual std::shared_ptr<FSDataInputStream> getStream() {
        if (creationException) {
            std::rethrow_exception(creationException);
        }
        return stream;
    }
    
    virtual ~StatePartitionStreamProvider() = default;
};

/**
 * This class provides access to an input stream that contains state data for one key group and the
 * key group id.
 */
class KeyGroupStatePartitionStreamProvider : public StatePartitionStreamProvider {
private:
    /** Key group that corresponds to the data in the provided stream */
    int keyGroupId;
    
public:
    /**
     * Creates a new KeyGroupStatePartitionStreamProvider that wraps a creation exception.
     */
    KeyGroupStatePartitionStreamProvider(std::exception_ptr creationException, int keyGroupId)
        : StatePartitionStreamProvider(creationException),
          keyGroupId(keyGroupId) {
    }
    
    /**
     * Creates a new KeyGroupStatePartitionStreamProvider that wraps an input stream.
     */
    KeyGroupStatePartitionStreamProvider(std::shared_ptr<FSDataInputStream> stream, int keyGroupId)
        : StatePartitionStreamProvider(stream),
          keyGroupId(keyGroupId) {
    }
    
    /**
     * Returns the key group that corresponds to the data in the provided stream.
     */
    int getKeyGroupId() const {
        return keyGroupId;
    }
    
    ~KeyGroupStatePartitionStreamProvider() override = default;
};

