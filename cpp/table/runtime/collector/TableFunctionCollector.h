//
// Created by xichen on 1/30/25.
//

#ifndef FLINK_TNEL_TABLEFUNCTIONCOLLECTOR_H
#define FLINK_TNEL_TABLEFUNCTIONCOLLECTOR_H

#include "functions/Collector.h"

class TableFunctionCollector : public Collector {
public:
    void setInput(void* input) {
        this->input = input;
    }
    void* getInput() {return input;}
    void reset();
    void outputResult(void* result);
    bool isCollected() const { return collected;}
    void close() override {
        this->collector->close();
    }
    void setCollector(Collector* collector_) {
        this->collector = collector_;
    }
    void collect(void* result) override {
        collector->collect(result);
        collected = true;
    }
private:
    bool collected;
    void* input;
    // The downstream
    Collector* collector;
};


#endif //FLINK_TNEL_TABLEFUNCTIONCOLLECTOR_H
