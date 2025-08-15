/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_STRING_H
#define FLINK_TNEL_STRING_H

#include <string_view>
#include <regex>
#include "Array.h"
#include "khsel_ops.h"

class String : public Object {
public:
    String();

    explicit String(const String *str);

    explicit String(const char *str, const size_t size);

    explicit String(const std::string &str);

    explicit String(std::string &&str) noexcept;

    String(const String &str);

    String(String &&str);

    String &operator=(const String &str);

    String &operator=(String &&str);

    ~String() override;

    virtual std::string_view getValue();

    virtual char *getData();

    virtual size_t getSize();

    void setValue(const std::string &val);

    void setValue(const std::string_view &val);

    void setData(const char *pointer);

    int hashCode() override;

    bool equals(Object *obj) override;

    std::string toString() override;

    Object *clone() override;

    void resize(const int64_t size);

    char *data();

    String *replace(const std::string &target, const std::string &replacement);

    String *replace(const String *target, const String *replacement);

    Array *split(const std::string &pattern);

    Array *split(const std::regex &re);

    Array *split(const String *patt);

    /* only work for replace pattern: "[^A-Za-z0-9_/.]+" */
    String *replaceAll_tune(const std::string &replace) const;

    String *replaceAll_tune(const std::string &pattern, const std::string &replace) const;

    String *replaceAll(const std::string &pattern, const std::string &replace) const;

    int32_t lastIndexOf(const std::string &s_patt) const;

    int32_t lastIndexOf(const String *s_patt) const;

    int32_t length() const;

    String *substring(const int32_t idx) const;

    String *substring(const int32_t start, const int32_t end) const;

    bool equals(const String *obj) const;

    bool equals(String* obj);

    bool equals(const std::string &str) const;

    bool contains(const std::string &str) const;

    bool contains(const String *str) const;

    bool endsWith(const String *str) const;

    bool endsWith(const std::string &str);

    bool startsWith(const String *str) const;

    bool startsWith(const std::string &str) const;

    static std::unique_ptr<String> valueOf(Object *obj);

    std::string_view ref();

    virtual void setData(char *pointer);

    virtual void setSize(size_t size);
private:
    std::string inner;
};

#endif // FLINK_TNEL_STRING_H
