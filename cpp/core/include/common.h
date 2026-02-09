/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef FLINK_TNEL_COMMON_H
#define FLINK_TNEL_COMMON_H

#include <iostream>
#include <chrono>
#include <stdexcept>
#include <iomanip>
#include <vector>
#include <mutex>
#include <thread>

inline std::mutex global_mutex;

std::string getFilenameFromPath(const char* filePath);

std::string u32string_to_std_string(const std::u32string& u32str);
std::u32string to_u32string(std::string_view sv);
std::string uint32_to_hex_string(uint32_t);
std::string bytesToHex(uint8_t* _bytes__, size_t _size__);
std::string Base64_encode(const std::vector<uint8_t>& input);
std::vector<uint8_t> Base64_decode(const std::string& encoded_string);

#define likely(x)      __builtin_expect(!!(x), 1)
#define unlikely(x)    __builtin_expect(!!(x), 0)

#define NOT_IMPL_EXCEPTION throw std::logic_error("To be implemented:" + std::string(__PRETTY_FUNCTION__));

#define THROW_LOGIC_EXCEPTION(msg) do { \
    std::stringstream ss; \
    ss << "[ #" << std::this_thread::get_id() << "] " ; \
    ss << msg; \
    throw std::logic_error(std::string(__PRETTY_FUNCTION__) + ss.str() ); \
} while (false);


#define THROW_RUNTIME_ERROR(msg) do { \
    std::stringstream ss; \
    ss << "[ #" << std::this_thread::get_id() << "] " ; \
    ss << msg; \
    throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ss.str() ) ; \
} while (false);

#define INFO_RELEASE(msg) do { \
    auto now = std::chrono::system_clock::now(); \
    time_t currentTime = std::chrono::system_clock::to_time_t(now); \
    char timeBuffer[80]; \
    strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", localtime(&currentTime)); \
    std::cout <<  "[" << "INFO" << "] [ #" << std::this_thread::get_id() << "] " << \
    timeBuffer <<  "  :" <<  ">>>>" <<  msg << std::endl; \
    std::cout.flush();  \
} while (false);

#ifdef DEBUG
#define INFO_RELEASE(msg)

#define LOG_INTERNAL(cat, msg) do { \
    std::lock_guard<std::mutex> lock(global_mutex); \
    auto now = std::chrono::system_clock::now(); \
    time_t currentTime = std::chrono::system_clock::to_time_t(now); \
    char timeBuffer[80]; \
    strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", localtime(&currentTime));     \
    std::cout <<  "[" << cat << "] [ #" << std::this_thread::get_id() << "] " << \
    timeBuffer <<  "  :" <<  getFilenameFromPath(__FILE__) << "(@" << __LINE__ << ") " << __PRETTY_FUNCTION__  \
    << " >>>> " << msg << std::endl; \
    std::cout.flush();  \
} while (false);

#define LOG_DEBUG(msg)   LOG_INTERNAL("DEBUG", msg)
#define LOG_INFO(msg)   LOG_INTERNAL("INFO", msg)
#define LOG_WARN(msg)   LOG_INTERNAL("WARN", msg)
#define LOG_ERROR(msg)   LOG_INTERNAL("ERROR", msg)

#define LOG(msg)
#define LOG_TRACE(msg)
#define LOG_PART(msg)
#define LOG_INFO_IMP(msg)

#define LOG_PRINTF(fmt, ...)
#define PRINT_HEX(bytebuffer, offset, length)

#define LOCK_BEFORE()
#define LOCK_AFTER()

#else

#define LOG(msg)
#define LOG_TRACE(msg)
#define LOG_PART(msg)
#define LOG_INFO_IMP(msg)

#define LOG_PRINTF(fmt, ...)
#define PRINT_HEX(bytebuffer, offset, length)

#define LOCK_BEFORE()
#define LOCK_AFTER()

#endif  // DEBUG


#ifdef DEBUG_MEM
#define STD_LOG(msg) do { \
                    std::lock_guard<std::mutex> lock(global_mutex); \
                    auto now = std::chrono::system_clock::now(); \
                    time_t currentTime = std::chrono::system_clock::to_time_t(now); \
                    char timeBuffer[80]; \
                    strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", localtime(&currentTime));     \
                    std::cout <<  "[ #" << std::this_thread::get_id() << "] " << \
                    timeBuffer <<  "  :" << __FILE__ << "(@" << __LINE__ << ") " << __PRETTY_FUNCTION__  \
                    << ": " <<  msg << std::endl; \
                    std::cout.flush();  \
                    } while (false);

#else
#define STD_LOG(msg)
#endif

void GErrorLog(std::string msg);

#endif
