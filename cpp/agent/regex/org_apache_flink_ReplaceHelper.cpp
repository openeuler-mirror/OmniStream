#include "org_apache_flink_ReplaceHelper.h"
#include <jni.h>
#define PCRE2_CODE_UNIT_WIDTH 16
#include <pcre2.h>
#include <arm_sve.h>
#include <cstdint>
#include <cstring>
#include <limits>
#include <list>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

constexpr size_t COMPILED_REGEX_CACHE_CAPACITY = 32;
constexpr uint64_t MODE_LITERAL_TO_SINGLE = 1;
constexpr uint64_t MODE_CLEAN_CONTROL_CHARS = 2;

class CompiledRegex {
public:
    CompiledRegex(const jchar *text, jsize length)
            : patternText_(reinterpret_cast<const char16_t *>(text),
                           static_cast<size_t>(length)),
              code_(nullptr),
              matchData_(nullptr) {
        int errorCode = 0;
        PCRE2_SIZE errorOffset = 0;
        code_ = pcre2_compile(
                reinterpret_cast<PCRE2_SPTR>(patternText_.data()),
                patternText_.size(),
                PCRE2_UTF,
                &errorCode,
                &errorOffset,
                nullptr);
        if (code_ == nullptr) {
            throw std::runtime_error("PCRE2 pattern compilation failed");
        }
        if (pcre2_jit_compile(code_, PCRE2_JIT_COMPLETE) != 0) {
            pcre2_code_free(code_);
            code_ = nullptr;
            throw std::runtime_error("PCRE2 JIT compilation failed");
        }
        matchData_ = pcre2_match_data_create_from_pattern(code_, nullptr);
        if (matchData_ == nullptr) {
            pcre2_code_free(code_);
            code_ = nullptr;
            throw std::bad_alloc();
        }
    }

    ~CompiledRegex() {
        if (matchData_ != nullptr) {
            pcre2_match_data_free(matchData_);
        }
        if (code_ != nullptr) {
            pcre2_code_free(code_);
        }
    }

    CompiledRegex(const CompiledRegex &) = delete;
    CompiledRegex &operator=(const CompiledRegex &) = delete;

    bool matches(const jchar *text, jsize length) const {
        return patternText_.size() == static_cast<size_t>(length)
               && std::memcmp(patternText_.data(), text,
                              static_cast<size_t>(length) * sizeof(jchar)) == 0;
    }

    pcre2_code *code() const {
        return code_;
    }

    pcre2_match_data *matchData() const {
        return matchData_;
    }

private:
    std::u16string patternText_;
    pcre2_code *code_;
    pcre2_match_data *matchData_;
};

class CompiledRegexCache {
public:
    CompiledRegex &get(const jchar *patternText, jsize patternLength) {
        for (auto it = entries_.begin(); it != entries_.end(); ++it) {
            if (it->matches(patternText, patternLength)) {
                entries_.splice(entries_.begin(), entries_, it);
                return entries_.front();
            }
        }

        entries_.emplace_front(patternText, patternLength);
        if (entries_.size() > COMPILED_REGEX_CACHE_CAPACITY) {
            entries_.pop_back();
        }
        return entries_.front();
    }

private:
    std::list<CompiledRegex> entries_;
};

CompiledRegex &getCompiledRegex(const jchar *patternText, jsize patternLength) {
    thread_local CompiledRegexCache cache;
    return cache.get(patternText, patternLength);
}

class JavaStringChars {
public:
    JavaStringChars(JNIEnv *env, jstring value)
            : env_(env),
              value_(value),
              length_(value == nullptr ? 0 : env->GetStringLength(value)),
              chars_(value == nullptr ? nullptr : env->GetStringChars(value, nullptr)) {
    }

    ~JavaStringChars() {
        if (chars_ != nullptr) {
            env_->ReleaseStringChars(value_, chars_);
        }
    }

    JavaStringChars(const JavaStringChars &) = delete;
    JavaStringChars &operator=(const JavaStringChars &) = delete;

    bool valid() const {
        return chars_ != nullptr;
    }

    const jchar *data() const {
        return chars_;
    }

    jsize size() const {
        return length_;
    }

private:
    JNIEnv *env_;
    jstring value_;
    jsize length_;
    const jchar *chars_;
};

size_t cleanControlCharsSVE_u16(uint16_t *data, size_t len) {
    size_t out_pos = 0;
    size_t i = 0;

    while (i < len) {
        svbool_t pg = svwhilelt_b16(i, len);
        svuint16_t chunk = svld1_u16(pg, data + i);

        svbool_t is_zero = svcmpeq_n_u16(pg, chunk, u'\0');
        svbool_t is_cr = svcmpeq_n_u16(pg, chunk, u'\r');
        svbool_t is_lf = svcmpeq_n_u16(pg, chunk, u'\n');
        svbool_t is_tab = svcmpeq_n_u16(pg, chunk, u'\t');
        svbool_t remove_mask =
                svorr_b_z(pg,
                          svorr_b_z(pg, is_zero, is_cr),
                          svorr_b_z(pg, is_lf, is_tab));

        size_t vl = svcnth();
        uint16_t data_buf[128];
        uint16_t mask_buf[128];

        svst1_u16(pg, data_buf, chunk);
        svuint16_t mask_u16 = svsel_u16(remove_mask, svdup_u16(1), svdup_u16(0));
        svst1_u16(pg, mask_buf, mask_u16);

        for (size_t j = 0; j < vl && (i + j) < len; ++j) {
            if (mask_buf[j] == 0) {
                data[out_pos++] = data_buf[j];
            }
        }
        i += vl;
    }
    return out_pos;
}

size_t replaceSingleCharSVE_u16(uint16_t *data, size_t len, uint16_t oldChar, uint16_t newChar) {
    size_t i = 0;

    while (i < len) {
        svbool_t pg = svwhilelt_b16(i, len);
        svuint16_t chunk = svld1_u16(pg, data + i);
        svbool_t match_mask = svcmpeq_n_u16(pg, chunk, oldChar);
        svuint16_t replaced = svsel_u16(match_mask, svdup_u16(newChar), chunk);

        svst1_u16(pg, data + i, replaced);
        i += svcnth();
    }
    return len;
}

size_t replaceLiteralToSingleSVE_u16(
        uint16_t *data,
        size_t len,
        uint8_t oldLen,
        uint16_t old1,
        uint16_t old2,
        uint16_t newChar) {
    if (oldLen == 1) {
        return replaceSingleCharSVE_u16(data, len, old1, newChar);
    }
    if (oldLen != 2) {
        return len;
    }

    size_t out_pos = 0;
    size_t i = 0;

    while (i < len) {
        svbool_t pg = svwhilelt_b16(i, len);
        svuint16_t chunk = svld1_u16(pg, data + i);
        svbool_t old1_mask = svcmpeq_n_u16(pg, chunk, old1);

        size_t vl = svcnth();
        uint16_t data_buf[128];
        uint16_t mask_buf[128];

        svst1_u16(pg, data_buf, chunk);
        svuint16_t mask_u16 = svsel_u16(old1_mask, svdup_u16(1), svdup_u16(0));
        svst1_u16(pg, mask_buf, mask_u16);

        size_t extra_advance = 0;
        for (size_t j = 0; j < vl && (i + j) < len; ++j) {
            if (mask_buf[j] != 0 && (i + j + 1) < len) {
                uint16_t next = (j + 1 < vl && (i + j + 1) < len)
                                ? data_buf[j + 1]
                                : data[i + j + 1];
                if (next == old2) {
                    data[out_pos++] = newChar;
                    if (j + 1 < vl) {
                        ++j;
                    } else {
                        extra_advance = 1;
                    }
                    continue;
                }
            }
            data[out_pos++] = data_buf[j];
        }
        i += vl + extra_advance;
    }
    return out_pos;
}

bool convertJavaReplacement(
        const jchar *replacement,
        jsize replacementLength,
        std::u16string *out,
        bool *literalReplacement) {
    out->clear();
    *literalReplacement = true;
    for (jsize i = 0; i < replacementLength; ++i) {
        jchar c = replacement[i];
        if (c == u'\\') {
            if (i + 1 >= replacementLength) {
                return false;
            }
            jchar literal = replacement[++i];
            if (literal == u'$') {
                out->append(u"$$");
                *literalReplacement = false;
            } else {
                out->push_back(static_cast<char16_t>(literal));
            }
        } else if (c == u'$') {
            *literalReplacement = false;
            if (i + 1 >= replacementLength) {
                return false;
            }
            jchar next = replacement[i + 1];
            if (next == u'{') {
                return false;
            }
            if (next < u'0' || next > u'9') {
                return false;
            }
            out->push_back(u'$');
        } else {
            out->push_back(static_cast<char16_t>(c));
        }
    }
    return true;
}

}

JNIEXPORT jint JNICALL Java_org_apache_flink_ReplaceHelper_nativeReplaceAll
        (JNIEnv *env, jclass clazz, jcharArray valueArray, jbyte coder, jlong actionCode) {
    jsize len = env->GetArrayLength(valueArray);
    jchar* data =
            (jchar*)env->GetPrimitiveArrayCritical(valueArray, NULL);

    uint64_t encodedAction = static_cast<uint64_t>(actionCode);
    uint8_t mode = static_cast<uint8_t>(encodedAction >> 56);
    size_t u16Len = len;
    if (mode == MODE_LITERAL_TO_SINGLE) {
        uint8_t oldLen = static_cast<uint8_t>((encodedAction >> 48) & 0xFF);
        uint16_t old1 = static_cast<uint16_t>((encodedAction >> 32) & 0xFFFF);
        uint16_t old2 = static_cast<uint16_t>((encodedAction >> 16) & 0xFFFF);
        uint16_t newChar = static_cast<uint16_t>(encodedAction & 0xFFFF);
        u16Len = replaceLiteralToSingleSVE_u16(
                (uint16_t *) data,
                len,
                oldLen,
                old1,
                old2,
                newChar);
    } else if (mode == MODE_CLEAN_CONTROL_CHARS) {
        u16Len = cleanControlCharsSVE_u16((uint16_t *) data, len);
    }

    env->ReleasePrimitiveArrayCritical(valueArray, data, 0);
    return static_cast<jint>(u16Len);
}

JNIEXPORT jstring JNICALL Java_org_apache_flink_ReplaceHelper_nativeReplaceAllGeneric
        (JNIEnv *env, jclass clazz, jstring input, jstring regex, jstring replacement) {
    try {
        JavaStringChars inputText(env, input);
        JavaStringChars regexText(env, regex);
        JavaStringChars replacementText(env, replacement);
        if (!inputText.valid() || !regexText.valid() || !replacementText.valid()) {
            return nullptr;
        }

        std::u16string nativeReplacement;
        bool literalReplacement = false;
        if (!convertJavaReplacement(
                replacementText.data(),
                replacementText.size(),
                &nativeReplacement,
                &literalReplacement)) {
            return nullptr;
        }

        CompiledRegex &pattern =
                getCompiledRegex(regexText.data(), regexText.size());
        size_t initialCapacity =
                static_cast<size_t>(inputText.size()) * 2
                + nativeReplacement.size() + 1;
        std::vector<PCRE2_UCHAR> output(initialCapacity);
        PCRE2_SIZE outputLength = output.size();
        uint32_t options =
                PCRE2_SUBSTITUTE_GLOBAL
                | PCRE2_SUBSTITUTE_UNSET_EMPTY
                | PCRE2_SUBSTITUTE_OVERFLOW_LENGTH;
        if (literalReplacement) {
            options |= PCRE2_SUBSTITUTE_LITERAL;
        }

        int result = pcre2_substitute(
                pattern.code(),
                reinterpret_cast<PCRE2_SPTR>(inputText.data()),
                static_cast<PCRE2_SIZE>(inputText.size()),
                0,
                options,
                pattern.matchData(),
                nullptr,
                reinterpret_cast<PCRE2_SPTR>(nativeReplacement.data()),
                nativeReplacement.size(),
                output.data(),
                &outputLength);

        if (result == PCRE2_ERROR_NOMEMORY && outputLength != PCRE2_UNSET) {
            output.resize(outputLength);
            PCRE2_SIZE retryLength = output.size();
            result = pcre2_substitute(
                    pattern.code(),
                    reinterpret_cast<PCRE2_SPTR>(inputText.data()),
                    static_cast<PCRE2_SIZE>(inputText.size()),
                    0,
                    options,
                    pattern.matchData(),
                    nullptr,
                    reinterpret_cast<PCRE2_SPTR>(nativeReplacement.data()),
                    nativeReplacement.size(),
                    output.data(),
                    &retryLength);
            outputLength = retryLength;
        }

        if (result < 0
            || outputLength > static_cast<PCRE2_SIZE>(
                    std::numeric_limits<jsize>::max())) {
            return nullptr;
        }
        if (result == 0) {
            return input;
        }
        return env->NewString(
                reinterpret_cast<const jchar *>(output.data()),
                static_cast<jsize>(outputLength));
    } catch (const std::exception &) {
        return nullptr;
    }
}
