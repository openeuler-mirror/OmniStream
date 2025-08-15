//
// Created by root on 8/18/24.
//

#ifndef FLINK_TNEL_COMMON_H
#define FLINK_TNEL_COMMON_H

#include <iostream>
#include <chrono>
#include <stdexcept>
#include <iomanip>
#include <mutex>
#include <thread>

inline std::mutex global_mutex;

std::string getFilenameFromPath(const char* filePath);

std::string u32string_to_std_string(const std::u32string& u32str);
std::u32string to_u32string(std::string_view sv);
std::string uint32_to_hex_string(uint32_t);
std::string bytesToHex(uint8_t* _bytes__, size_t _size__);

#define likely(x)      __builtin_expect(!!(x), 1)
#define unlikely(x)    __builtin_expect(!!(x), 0)

#define NOT_IMPL_EXCEPTION throw std::logic_error("To be implemented:" + std::string(__PRETTY_FUNCTION__) );

#define THROW_LOGIC_EXCEPTION(msg) do { \
                                            std::stringstream ss; \
                                            ss << "[ #" << std::this_thread::get_id() << "] " ; \
                                            ss << msg; \
                                            throw std::logic_error(std::string(__PRETTY_FUNCTION__) + ss.str() ) ;\
                                       } while (false);


#define THROW_RUNTIME_ERROR(msg) do { \
                                        std::stringstream ss; \
                                        ss << "[ #" << std::this_thread::get_id() << "] " ; \
                                        ss << msg; \
                                        throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ss.str() ) ;\
                                        } while (false);


#ifdef DEBUG


#define LOG_INTERNAL(cat, msg) do { \
std::lock_guard<std::mutex> lock(global_mutex); \
auto now = std::chrono::system_clock::now(); \
time_t currentTime = std::chrono::system_clock::to_time_t(now); \
char timeBuffer[80];\
strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", localtime(&currentTime));     \
std::cout <<  "[" << cat << "] [ #" << std::this_thread::get_id() << "] " << \
timeBuffer <<  "  :" <<  getFilenameFromPath(__FILE__) << "(@" << __LINE__ << ") " << __PRETTY_FUNCTION__  \
<<  ">>>>" <<  msg << std::endl; \
std::cout.flush();  \
} while( false );

/**
#define LOG(msg) do { \
                        std::lock_guard<std::mutex> lock(global_mutex); \
                        auto now = std::chrono::system_clock::now(); \
                        time_t currentTime = std::chrono::system_clock::to_time_t(now); \
                        char timeBuffer[80];\
                        strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", localtime(&currentTime));     \
                        std::cout <<  "[ #" << std::this_thread::get_id() << "] " << \
                        timeBuffer <<  "  :" << __FILE__ << "(@" << __LINE__ << ") " << __PRETTY_FUNCTION__  \
                        << ": " <<  msg << std::endl; \
                        std::cout.flush();  \
                     } while( false );
**/

#define LOG(msg)         LOG_TRACE(msg)
#define LOG_TRACE(msg)   LOG_INTERNAL( "[______TRACE_____]"  ,  msg )
#define LOG_PART(msg)     LOG_INTERNAL( "[___PARTITION____]"  ,msg )
#define LOG_INFO_IMP(msg) LOG_INTERNAL( "[____INFO____]"  , msg )
#define LOCK_BEFORE()    LOG_INTERNAL( "LOCK", "[LOCK_BEFORE]" )
#define LOCK_AFTER()    LOG_INTERNAL( "LOCK", "[LOCK_AFTER]" )

#define LOG_PRINTF(fmt, ...) printf("[LOG] " fmt "\n", ##__VA_ARGS__)

#define PRINT_HEX(bytebuffer, offset, length) do { \
                                                    std::lock_guard<std::mutex> lock(global_mutex); \
                                                    std::string hexStr_ = bytesToHex(bytebuffer + offset, length) ;\
                                                    auto now = std::chrono::system_clock::now(); \
                                                    time_t currentTime = std::chrono::system_clock::to_time_t(now); \
                                                    char timeBuffer[80];\
                                                    strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", localtime(&currentTime));     \
                                                    std::cout <<  "[ #" << std::this_thread::get_id() << "] " << \
                                                    timeBuffer <<  "  :" << __FILE__ << "(@" << __LINE__ << ") " << __PRETTY_FUNCTION__  \
                                                    << ": offset " << offset << " sizeInBytes " << length << " Hex Value:  " <<  hexStr_ << std::endl; \
                                                    std::cout.flush();  \
                                                }  while (false);


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
                    char timeBuffer[80];\
                    strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", localtime(&currentTime));     \
                    std::cout <<  "[ #" << std::this_thread::get_id() << "] " << \
                    timeBuffer <<  "  :" << __FILE__ << "(@" << __LINE__ << ") " << __PRETTY_FUNCTION__  \
                    << ": " <<  msg << std::endl; \
                    std::cout.flush();  \
                    } while( false );

#else
#define STD_LOG(msg)
#endif

#define INFO_RELEASE(msg) do { \
                auto now = std::chrono::system_clock::now(); \
                time_t currentTime = std::chrono::system_clock::to_time_t(now); \
                char timeBuffer[80];\
                strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", localtime(&currentTime));     \
                std::cout <<  "[" << "INFO" << "] [ #" << std::this_thread::get_id() << "] " << \
                timeBuffer <<  "  :" <<  ">>>>" <<  msg << std::endl; \
                std::cout.flush();  \
                } while( false );

void GErrorLog(std::string msg);


#endif //FLINK_TNEL_UTILS_H