//
// Created by Yuhe on 2025-02-25.
//

#ifndef OMNISTREAM_STREAMSOURCE_H
#define OMNISTREAM_STREAMSOURCE_H

#include <memory>

#include "core/operators/AbstractStreamOperator.h"
#include "core/operators/AbstractUdfStreamOperator.h"
#include "core/streamrecord/StreamRecord.h"
#include "core/typeinfo/TypeInformation.h"
#include "functions/SourceContext.h"
#include "table/runtime/operators/source/InputFormatSourceFunction.h"
#include "table/runtime/operators/source/InputSplit.h"
#include "jni.h"
#include "basictypes/callback.h"
#include "operators/source/StreamSourceContexts.h"
#include "runtime/tasks/SystemProcessingTimeService.h"
#include "udf/UDFLoader.h"

namespace omnistream {
template<typename K>
class StreamSource : public AbstractUdfStreamOperator<SourceFunction<K>, K*> {
public:
    StreamSource(SourceFunction<K> *func, Output *output) : AbstractUdfStreamOperator<SourceFunction<K>, K*>(func, output) {
        function = SourceFunctionUnique<K>(func);
    }

    StreamSource(Output *output, nlohmann::json config) {
        this->output = output;
        loadUdf(config);
    }

     ~StreamSource() {
        delete ctx;
    }

    void loadUdf(const nlohmann::json &config) {
        std::string soPath = config["udf_so"];
        std::string udfObj = config["udf_obj"];
        nlohmann::json udfObjJson = nlohmann::json::parse(udfObj);

        auto *symbol = udfLoader.LoadSourceFunction(soPath);
        if (symbol == nullptr) {
            throw std::out_of_range("null pointer when load " + soPath);
        }
        function = symbol(udfObjJson);
        this->userFunction = function.get();
        isStream = true;
    }

    void setProcessArgs(jmethodID methodID, JNIEnv *env, jobject task) {
        CallBack *callback = new CallBack();
        callback->SetArgs(methodID, env, task);
        function->SaveCallBack(callback);
    }

    void run()
    {
        thread_local Object lockingObject;
        ctx = StreamSourceContexts::getSourceContext(
                TimeCharacteristic::ProcessingTime,
                new SystemProcessingTimeService(),
                &lockingObject,
                this->output,
                -1,
                -1,
                true,
                isStream);
        function->run(ctx);
    }

    // void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override
    //{}

    // void open() override
    // {}

    void cancel()
    {
        function->cancel();
    }
private:
    UDFLoader udfLoader;
    SourceFunctionUnique<K> function;
    SourceContext *ctx;
    bool isStream = false; // Optimization will be considered in the future.
};
}  // namespace omnistream

#endif  // OMNISTREAM_STREAMSOURCE_H