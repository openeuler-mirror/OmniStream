
/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
/*
//
// Created by root on 8/15/24.
//
#include <iostream>
#include "OperatorChain.h"
#include "../typeinfo/TypeInformation.h"
#include "../typeinfo/TypeInfoFactory.h"
#include "../operators/StreamOperatorFactory.h"
#include "ChainingOutput.h"


// for convenience
using json = nlohmann::json;

OperatorChain::OperatorChain(std::vector<OperatorConfig> & opChainConfig, std::shared_ptr<::RecordWriterDelegate> recordWriterDelegate) :
  opChainConfig_(opChainConfig), recordWriterDelegate_(recordWriterDelegate){
}

OperatorChain::~OperatorChain() {
    delete mainOperatorWrapper;
}

::RecordWriterOutput*  OperatorChain::createStreamOutput(::RecordWriter* recordWriter, NonChainedOutput *streamOutput, TypeInformation & typeInformation) {

    LOG("typeInformation.name()"   <<  typeInformation.name())
    TypeSerializer*  serializer = typeInformation.createTypeSerializer("TBD");
    LOG("After creation of serializer "   <<  serializer->getName())

    //reserved for future
    streamOutput;

    return new ::RecordWriterOutput( serializer,  recordWriter);
}

::RecordWriterOutput*  OperatorChain::createChainOutputs(NonChainedOutput *streamOutput, std::shared_ptr<::RecordWriterDelegate> recordWriterDelegate,
                                       std::vector<OperatorConfig> &opChainConfig) {
    TypeInformation*  chainOutputType = getChainOutputType(opChainConfig);

    LOG("Before call  createStreamOutput ")

    auto recordWriterOutput =
                                  createStreamOutput(
                                          recordWriterDelegate->getRecordWriter(0),
                                          streamOutput,
                                          *chainOutputType);
    LOG("Before delete chainOutputType ")
    delete chainOutputType;

    LOG("After delete chainOutputType ");
    return recordWriterOutput;
}

TypeInformation*  OperatorChain::getChainOutputType (std::vector<OperatorConfig> &opChainConfig) {

    LOG("Beginning of  getChainOutputType ")

    OperatorConfig opConfig = opChainConfig[opChainConfig.size() -1];

    LOG("after  getOperatorConfig");

    const json &outputType =  opConfig.getOutputType();

    LOG("after  getOperatorConfig:" + outputType.dump(2));

    TypeInformation* typeInfo;

    if (outputType["kind"] == "basic") {
        std::string inputTypeName = outputType["type"];
        typeInfo = TypeInfoFactory::createTypeInfo(inputTypeName.c_str(), nullptr);
    } else if (outputType["kind"] == "Row") {
        typeInfo = TypeInfoFactory::createInternalTypeInfo(outputType["type"]);
    } else {
        THROW_LOGIC_EXCEPTION("Unknown Input type" + to_string(outputType));
    }

    LOG("after  createTypeInfo");

    return typeInfo;
}

StreamOperator *
OperatorChain::createMainOperatorAndCollector(std::vector<OperatorConfig> &opChainConfig, ::RecordWriterOutput *chainOutput) {
    //TODO: we currently consider it is a line structure.
    //operatorA--OperatorB---OperatorC
    //Generating the last operator first and wrap it with the RecordWriterOutput.
    LOG(">> chaining with " << opChainConfig.size() << " operators...")
    OperatorConfig opConfig = opChainConfig[opChainConfig.size()-1];
    StreamOperator * op= StreamOperatorFactory::createOperatorAndCollector(opConfig, chainOutput);
    //create tailStreamOperatorWrapper here
    tailOperatorWrapper= new StreamOperatorWrapper(op,false);
    auto nextOpWrapper= tailOperatorWrapper;
    for (int i = opChainConfig.size()-2; i >= 0; i--){
        LOG(">> generating chainingOutput" + i)
        ChainingOutput *chainingOutput= new ChainingOutput(static_cast<OneInputStreamOperator *>(op));
        OperatorConfig opConfig = opChainConfig[i];
        LOG(">> generating operator "+ opChainConfig[i].getUniqueName() +" and wrap the chaingOutput ")
        op= StreamOperatorFactory::createOperatorAndCollector(opConfig, chainingOutput);
        //create StreamOperatorWrapper here
        auto OpWrapper = new StreamOperatorWrapper(op,false);
        OpWrapper->setNext(nextOpWrapper);
        nextOpWrapper->setPrevious(OpWrapper);
        nextOpWrapper=OpWrapper;
    }
    mainOperatorWrapper=nextOpWrapper;
    mainOperatorWrapper->setAsHead();
    //set the last StreamOperatorWrapper as the mainStreamOperatorWrapper.
    return op;
}

void OperatorChain::initializeStateAndOpenOperators(StreamTaskStateInitializerImpl *initializer){
    //call operators' initializeState() and open() in a reverse order.
    auto opWrap= tailOperatorWrapper;
    while(opWrap!= nullptr){
        auto op= opWrap->getStreamOperator();
        // TODO: Determine Keyserializer from config.
        op->initializeState(initializer, new LongSerializer() */
/*temporary*//*
);
        op->open();
        opWrap = opWrap->getPrevious();
    }
}

*/
