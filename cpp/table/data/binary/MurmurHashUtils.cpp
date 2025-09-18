#include "MurmurHashUtils.h"
#include <libboundscheck/include/securec.h>

int MurmurHashUtils::hashUnsafeBytesByWords(const void* base, std::size_t offset, int lengthInBytes) {
    return hashUnsafeBytesByWords(base, offset, lengthInBytes, DEFAULT_SEED);
}

int MurmurHashUtils::hashUnsafeBytes(const void* base, std::size_t offset, int lengthInBytes) {
    return hashUnsafeBytes(base, offset, lengthInBytes, DEFAULT_SEED);
}

int MurmurHashUtils::hashBytesByWords(const uint8_t* segment, int offset, int lengthInBytes) {
    return hashBytesByWords(segment, offset, lengthInBytes, DEFAULT_SEED);
}

int MurmurHashUtils::hashBytes(const uint8_t* segment, int offset, int lengthInBytes) {
    return hashBytes(segment, offset, lengthInBytes, DEFAULT_SEED);
}

int MurmurHashUtils::hashUnsafeBytesByWords(const void* base, std::size_t offset, int lengthInBytes, int seed) {
    int h1 = hashUnsafeBytesByInt(base, offset, lengthInBytes, seed);
    return fmix(h1, lengthInBytes);
}

int MurmurHashUtils::hashBytesByWords(const uint8_t* segment, int offset, int lengthInBytes, int seed) {
    int h1 = hashBytesByInt(segment, offset, lengthInBytes, seed);
    return fmix(h1, lengthInBytes);
}

int MurmurHashUtils::hashBytes(const uint8_t* segment, int offset, int lengthInBytes, int seed) {
    int lengthAligned = lengthInBytes - lengthInBytes % 4;
    int h1 = hashBytesByInt(segment, offset, lengthAligned, seed);
    for (int i = lengthAligned; i < lengthInBytes; i++) {
        int k1 = mixK1(segment[offset + i]);
        h1 = mixH1(h1, k1);
    }
    return fmix(h1, lengthInBytes);
}

int MurmurHashUtils::hashUnsafeBytes(const void* base, std::size_t offset, int lengthInBytes, int seed) {
    assert(lengthInBytes >= 0);
    int lengthAligned = lengthInBytes - lengthInBytes % 4;
    int h1 = hashUnsafeBytesByInt(base, offset, lengthAligned, seed);
    for (int i = lengthAligned; i < lengthInBytes; i++) {
        int halfWord = static_cast<const uint8_t*>(base)[offset + i];
        int k1 = mixK1(halfWord);
        h1 = mixH1(h1, k1);
    }
    return fmix(h1, lengthInBytes);
}

void MurmurHashUtils::getResData(void* dst, void* src, size_t cur, int res)
{
    svbool_t pg = svwhilelt_b8(0, res);
    svuint8_t s = svld1(pg, reinterpret_cast<uint8_t*>(src) + cur);
    svst1(pg, reinterpret_cast<uint8_t*>(dst) + cur, s);
}

int MurmurHashUtils::hashUnsafeBytesByInt(const void* base, std::size_t offset, int lengthInBytes, int seed) {
    assert(lengthInBytes % 4 == 0);
    int h1 = seed;
    for (int i = 0; i < lengthInBytes; i += 4) {
        int halfWord;

        size_t len = sizeof(int);
        size_t skip_num = svcntb();
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, static_cast<uint8_t*>(const_cast<void*>(base)) + offset + i + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(&halfWord) + cur, s);
            cur += skip_num;
        }
        getResData(&halfWord, static_cast<uint8_t*>(const_cast<void*>(base)) + offset + i, cur, len - cur);
        int k1 = mixK1(halfWord);
        h1 = mixH1(h1, k1);
    }
    return h1;
}

int MurmurHashUtils::hashBytesByInt(const uint8_t* segment, int offset, int lengthInBytes, int seed) {
    assert(lengthInBytes % 4 == 0);
    int h1 = seed;
    for (int i = 0; i < lengthInBytes; i += 4) {
        int halfWord;

        size_t len = sizeof(int);
        size_t skip_num = svcntb();
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t j = 0; j < num; j++)
        {
            svuint8_t s = svld1(pTrue, const_cast<uint8_t*>(segment + offset + j) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(&halfWord) + cur, s);
            cur += skip_num;
        }
        getResData(&halfWord, const_cast<uint8_t*>(segment + offset + i), cur, len - cur);

        int k1 = mixK1(halfWord);
        h1 = mixH1(h1, k1);
    }
    return h1;
}

int MurmurHashUtils::mixK1(int k1) {
    k1 *= C1;
    k1 = (k1 << 15) | (static_cast<unsigned int>(k1) >> (32 - 15));
    k1 *= C2;
    return k1;
}

int MurmurHashUtils::mixH1(int h1, int k1) {
    h1 ^= k1;
    h1 = (h1 << 13) | (static_cast<unsigned int>(h1) >> (32 - 13));
    h1 = h1 * 5 + 0xe6546b64;
    return h1;
}

int MurmurHashUtils::fmix(int h1, int length) {
    h1 ^= length;
    return fmix(h1);
}

int MurmurHashUtils::fmix(int h) {
    h ^= static_cast<unsigned int>(h) >> 16;
    h *= 0x85ebca6b;
    h ^= static_cast<unsigned int>(h) >> 13;
    h *= 0xc2b2ae35;
    h ^= static_cast<unsigned int>(h) >> 16;
    return h;
}

std::uint64_t MurmurHashUtils::fmix(std::uint64_t h) {
    h ^= static_cast<unsigned long long>(h) >> 33;
    h *= 0xff51afd7ed558ccdL;
    h ^= static_cast<unsigned long long>(h) >> 33;
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= static_cast<unsigned long long>(h) >> 33;
    return h;
}