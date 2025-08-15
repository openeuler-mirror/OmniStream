#pragma once

#include <cstdint>
#include <cassert>
#include <cstring>
#include <arm_sve.h>

class MurmurHashUtils {
public:
    static const int DEFAULT_SEED = 42;

    static int hashUnsafeBytesByWords(const void* base, std::size_t offset, int lengthInBytes);
    static int hashUnsafeBytes(const void* base, std::size_t offset, int lengthInBytes);
    static int hashBytesByWords(const uint8_t* segment, int offset, int lengthInBytes);
    static int hashBytes(const uint8_t* segment, int offset, int lengthInBytes);

private:
    static const int C1 = 0xcc9e2d51;
    static const int C2 = 0x1b873593;

    MurmurHashUtils() = delete;

    static int hashUnsafeBytesByWords(const void* base, std::size_t offset, int lengthInBytes, int seed);
    static int hashBytesByWords(const uint8_t* segment, int offset, int lengthInBytes, int seed);
    static int hashBytes(const uint8_t* segment, int offset, int lengthInBytes, int seed);
    static int hashUnsafeBytes(const void* base, std::size_t offset, int lengthInBytes, int seed);
    static int hashUnsafeBytesByInt(const void* base, std::size_t offset, int lengthInBytes, int seed);
    static int hashBytesByInt(const uint8_t* segment, int offset, int lengthInBytes, int seed);
    static int mixK1(int k1);
    static int mixH1(int h1, int k1);
    static int fmix(int h1, int length);
    static int fmix(int h);
    static std::uint64_t fmix(std::uint64_t h);
    static void getResData(void* dst, void* src, size_t cur, int res);
};