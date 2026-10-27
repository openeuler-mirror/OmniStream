#include <jni.h>
#include <exception>
#include "runtime/state/NativeRocksDBResourceRegistry.h"

extern "C" JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_state_OmniRocksDBNativeBridge_createSharedResources(
    JNIEnv* env, jclass, jlong bytes, jdouble writeRatio, jdouble priorityRatio, jboolean partitioned)
{
    try {
        return NativeRocksDBResourceRegistry::instance().create(bytes, writeRatio, priorityRatio, partitioned);
    } catch (const std::exception& e) {
        env->ThrowNew(env->FindClass("java/lang/IllegalArgumentException"), e.what());
    } catch (...) {
        env->ThrowNew(env->FindClass("java/lang/IllegalStateException"), "Unable to create Omni RocksDB resources");
    }
    return 0;
}

extern "C" JNIEXPORT void JNICALL
Java_com_huawei_omniruntime_flink_runtime_state_OmniRocksDBNativeBridge_releaseSharedResources(
    JNIEnv* env, jclass, jlong handle)
{
    try {
        NativeRocksDBResourceRegistry::instance().release(handle);
    } catch (const std::exception& e) {
        env->ThrowNew(env->FindClass("java/lang/IllegalStateException"), e.what());
    } catch (...) {
        env->ThrowNew(env->FindClass("java/lang/IllegalStateException"), "Unable to release Omni RocksDB resources");
    }
}
