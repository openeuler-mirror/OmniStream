#pragma once

#include <utility>

#include "common.h"
#include "TypeInformation.h"
#include "typeconstants.h"
#include "table/runtime/operators/window/TimeWindow.h"

class WindowTypeInfo : public TypeInformation {
public:
    explicit WindowTypeInfo(std::string serializerInstanceClazz)
        : serializerInstanceClazz_(std::move(serializerInstanceClazz))
    {
        if (isTimeWindow()) {
            initInfo(TimeWindow::Serializer::SERIALIZER_NAME, TimeWindow::Serializer::SERIALIZER_BACKEND_DATA_TYPE);
        } else {
            THROW_RUNTIME_ERROR("Unsupported window class " << serializerInstanceClazz_);
        }
    }

    ~WindowTypeInfo() override = default;

    TypeSerializer* createTypeSerializer() override
    {
        if (isTimeWindow()) {
            return new TimeWindow::Serializer();
        } else {
            THROW_RUNTIME_ERROR("Unsupported window class " << serializerInstanceClazz_);
        }
    }

    std::string name() override
    {
        return name_;
    }

    BackendDataType getBackendId() const override
    {
        return backendDataType_;
    }

    const std::string& getSerializerInstanceClazz() const
    {
        return serializerInstanceClazz_;
    }

private:
    // fillInfo 方法用于填充序列化器的信息
    void initInfo(std::string name, BackendDataType backendDataType)
    {
        name_ = name;
        backendDataType_ = backendDataType;
    }

    bool isTimeWindow() const
    {
        return serializerInstanceClazz_ == TYPE_NAME_TIME_WINDOW_CLASS ||
               serializerInstanceClazz_ == TYPE_NAME_TIME_WINDOW_CLASS_LINE;
    }

    std::string serializerInstanceClazz_;
    std::string name_;
    BackendDataType backendDataType_;
};
