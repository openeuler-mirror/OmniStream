//
// Created by xichen on 1/30/25.
//

#include "TableFunctionCollector.h"

void TableFunctionCollector::reset() {
    collected = false;
    /* todo: This if is weird, it will have infinitely deep recursive call.
    if (auto *castedCollector = static_cast<TableFunctionCollector *>(collector)) {
        castedCollector->reset();
    }
     */
}
void TableFunctionCollector::outputResult(void* result) {
    collected = true;
    this->collector->collect(result);
}
