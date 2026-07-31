#include "org_apache_flink_Crc32Helper.h"
#include <arm_acle.h>
#include <cstdint>
#include <cstring>

/**
 * CRC32 using ARM hardware crc32d instruction.
 * Polynomial: 0xEDB88320 (standard zip/gzip CRC32, same as Kafka Crc32 table).
 * 
 * crc32d: CRC32 of 64-bit word
 * crc32w: CRC32 of 32-bit word
 * crc32b: CRC32 of 8-bit byte
 */
JNIEXPORT jint JNICALL Java_org_apache_flink_Crc32Helper_nativeCrc32
        (JNIEnv* env, jclass cls, jbyteArray data, jint offset, jint len)
{
    jboolean isCopy;
    jbyte* bytes = static_cast<jbyte*>(env->GetPrimitiveArrayCritical(data, &isCopy));
    if (!bytes) return 0;

    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(bytes + offset);
    size_t n = static_cast<size_t>(len);
    uint32_t crc = ~0u;
    size_t i = 0;

    // 64-byte unrolled loop: 8 x 8-byte crc32d
    while (i + 64 <= n) {
        crc = __crc32d(crc, *reinterpret_cast<const uint64_t*>(ptr + i)); i += 8;
        crc = __crc32d(crc, *reinterpret_cast<const uint64_t*>(ptr + i)); i += 8;
        crc = __crc32d(crc, *reinterpret_cast<const uint64_t*>(ptr + i)); i += 8;
        crc = __crc32d(crc, *reinterpret_cast<const uint64_t*>(ptr + i)); i += 8;
        crc = __crc32d(crc, *reinterpret_cast<const uint64_t*>(ptr + i)); i += 8;
        crc = __crc32d(crc, *reinterpret_cast<const uint64_t*>(ptr + i)); i += 8;
        crc = __crc32d(crc, *reinterpret_cast<const uint64_t*>(ptr + i)); i += 8;
        crc = __crc32d(crc, *reinterpret_cast<const uint64_t*>(ptr + i)); i += 8;
    }

    while (i + 8 <= n) {
        crc = __crc32d(crc, *reinterpret_cast<const uint64_t*>(ptr + i));
        i += 8;
    }
    if (i + 4 <= n) {
        crc = __crc32w(crc, *reinterpret_cast<const uint32_t*>(ptr + i));
        i += 4;
    }
    while (i < n) {
        crc = __crc32b(crc, ptr[i]);
        i++;
    }

    env->ReleasePrimitiveArrayCritical(data, bytes, JNI_ABORT);
    return static_cast<jint>(~crc);
}
