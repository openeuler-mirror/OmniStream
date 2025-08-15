//
// Created by root on 8/20/24.
//
#include <iostream>
#include <locale>
#include <codecvt>
#include "common.h"

std::string u32string_to_std_string(const std::u32string &u32str) {
    std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t> utf32_converter;
    return utf32_converter.to_bytes(u32str);
}

std::string uint32_to_hex_string(uint32_t value) {
    std::stringstream ss;
    ss << std::hex << value;
    return ss.str();
}

std::string bytesToHex(uint8_t * _bytes__, size_t _size__) {
    std::stringstream ss;
    ss << std::hex << std::uppercase  << std::setfill('0');
    for (size_t i = 0; i < _size__; ++i) {
        ss << std::setw(2) <<  static_cast<int>(_bytes__[i]) << " ";
    }
    return  ss.str();
}

std::u32string to_u32string(std::string_view sv) {
    std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t> converter;
    return converter.from_bytes(sv.data(), sv.data() + sv.size());
}

std::string getFilenameFromPath(const char* filePath){
    std::string path(filePath);
    size_t lastSlash = path.find_last_of("/\\");

    if (lastSlash == std::string::npos) {
        return path; // No slash found, so it's just the filename
    } else {
        return path.substr(lastSlash + 1);
    }
}

void GErrorLog(std::string msg)
{
    const int fixedStringLen = 80;
    auto now = std::chrono::system_clock::now();
    time_t currentTime = std::chrono::system_clock::to_time_t(now);
    char timeBuffer[fixedStringLen];
    strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", localtime(&currentTime));
    std::cout <<  "[" << "ERROR" << "] [ #" << std::this_thread::get_id() << "] " <<
    timeBuffer <<  "  :" <<  ">>>>" <<  (msg) << std::endl;
    std::cout.flush();
}