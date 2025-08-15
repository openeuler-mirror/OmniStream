//
// Created by xichen on 1/29/25.
//

#include "CsvTableSource.h"

size_t CsvTableSource::countCsvRows()  {
    std::ifstream file(filepath, std::ios::binary);
    if (!file) {
        LOG("Error opening file: " + filepath);
        return 0;
    }
    size_t line_count = 0;
    char buffer[8192];

    while (file.read(buffer, sizeof(buffer))) {
        line_count += std::count(buffer, buffer + file.gcount(), '\n');
    }
    // Count remaining characters in the last read
    line_count += std::count(buffer, buffer + file.gcount(), '\n');

    return line_count;
}
