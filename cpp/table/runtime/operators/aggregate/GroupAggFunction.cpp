/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "GroupAggFunction.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "runtime/generated/function/AverageFunction.h"
#include "runtime/generated/function/CountDistinctFunction.h"
#include "runtime/dataview/PerKeyStateDataViewStore.h"
#include "runtime/generated/function/CountFunction.h"
#include "runtime/generated/function/MinMaxFunction.h"
#include "runtime/generated/function/SumFunction.h"
#include "runtime/generated/function/udf/LastStringValueFunction.h"
#include <iostream>
#include <regex>

GroupAggFunction::GroupAggFunction(long stateRetentionTime,
                                   const nlohmann::json& config)
    : stateRetentionTime(stateRetentionTime),
      description(config) {
    indexOfCountStar = config["aggInfoList"]["indexOfCountStar"];
    recordCounter = std::move(RecordCounter::of(indexOfCountStar));
    accTypes = config["aggInfoList"]["accTypes"].get<std::vector<std::string>>();
    accTypes.erase(std::remove_if(accTypes.begin(), accTypes.end(),
                                  [](const std::string& type) { return type.find("RAW") != std::string::npos; }),
                   accTypes.end());
    aggValueTypes = config["aggInfoList"]["aggValueTypes"].get<std::vector<std::string>>();
    accumulatorArity = accTypes.size();
    generateUpdateBefore = config.value("generateUpdateBefore", false);
    inputTypes = config["inputTypes"].get<std::vector<std::string>>();
    outputTypes = config["outputTypes"].get<std::vector<std::string>>();
    keyedIndex = config["grouping"].get<std::vector<int32_t>>();
    keyedTypes = getKeyedTypes(keyedIndex, config["inputTypes"]);
    groupByKeySelector = new KeySelector<RowData*>(keyedTypes, keyedIndex);
    distinctInfos = config["distinctInfos"].get<std::vector<DistinctInfo>>();
}

GroupAggFunction::~GroupAggFunction()
{
    for (auto func : functions) {
        delete func;
    }
}

bool IntEqualiser(RowData* r1, RowData* r2, int colIdx)
{
    return *r1->getInt(colIdx) == *r2->getInt(colIdx);
}

bool LongEqualiser(RowData* r1, RowData* r2, int colIdx)
{
    return *r1->getLong(colIdx) == *r2->getLong(colIdx);
}

bool TimestampEqualiser(RowData* r1, RowData* r2, int colIdx)
{
    return *r1->getLong(colIdx) == *r2->getLong(colIdx);
}

std::string extractAggFunction(const std::string& input)
{
    std::regex aggRegex(R"((?:MAX|COUNT|SUM|MIN|AVG|last_string_value_without_retract))", std::regex_constants::icase);
    std::smatch match;
    if (std::regex_search(input, match, aggRegex)) {
        return match.str();
    } else {
        return "NONE";
    }
}

std::vector<std::string> GroupAggFunction::handleInputTypes()
{
    std::vector<std::string> types;
    for (const std::string &inputType: description["inputTypes"]) {
        types.push_back(inputType);
        auto typeId = LogicalType::flinkTypeToOmniTypeId(inputType);
        if (typeId == DataTypeId::OMNI_INT) {
            equalisers.push_back(IntEqualiser);
        } else if (typeId == DataTypeId::OMNI_LONG) {
            equalisers.push_back(LongEqualiser);
        } else if (typeId == DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE ||
                   typeId == DataTypeId::OMNI_TIMESTAMP) {
            equalisers.push_back(TimestampEqualiser);
        } else {
            equalisers.push_back(nullptr);
            LOG("Warning equaliser for type " + inputType + " is not supported yet");
        }
    }
    return types;
}

std::map<int, int> GroupAggFunction::handleDistinctInfo()
{
    std::map<int, int> distinctInfoMap;
    for (DistinctInfo info: distinctInfos) {
        if (info.filterArgs.size() != info.aggIndexes.size()) {
            std::cerr << "Error: filterArgs and aggIndexes size mismatch!" << std::endl;
            continue;
        }
        for (size_t i = 0; i < info.filterArgs.size(); i++) {
            distinctInfoMap[info.aggIndexes[i]] = info.filterArgs[i];
        }
    }
    return distinctInfoMap;
}

void GroupAggFunction::open(const Configuration& parameters)
{
    LOG("GroupAggFunction open() running")
    // Init HeapValueState
    RowType accRowType(true, this->accTypes);
    auto accRowTypeInfo = InternalTypeInfo::ofRowType(&accRowType);
    std::string accStateName = "accState";
    ValueStateDescriptor *accDesc = new ValueStateDescriptor(accStateName, accRowTypeInfo);
    accDesc->SetStateSerializer(accRowTypeInfo->getTypeSerializer());

    // This kind of specific template type should all be solved by an if-else based on stateDescription
    accState = static_cast<StreamingRuntimeContext<RowData*> *>(getRuntimeContext())->getState<RowData*>(accDesc);
    int accStartingIndex = 0;
    int aggValueIndex = 0;
    InitAggFunctions(accStartingIndex, aggValueIndex);

    // When the value of consumeRetraction of any function in aggregateCalls is true,
    // that is, the input stream may contain a pullback message,
    // indexOfCountStar will not be -1 ，which is mainly used for the COUNT(*) operation.
    // In this case, we initialize an extra count function.
    if (indexOfCountStar != -1) {
        auto *function = new CountFunction(-1, "BIGINT", accStartingIndex, -1, -1);
        function->setCountStart(true);
        functions.push_back(function);
        accStartingIndex++;
    }

    LOG("group agg accStartingIndex: "<<accStartingIndex)
    LOG("group agg accumulatorArity: "<<accumulatorArity)
    assert(accStartingIndex == accumulatorArity);
    assert(aggValueIndex == static_cast<int>(aggValueTypes.size()));

    aggregateCallsCount = description["aggInfoList"]["aggregateCalls"].size();
    resultRow = new JoinedRowData();
    reUsePrevAggValue = BinaryRowData::createBinaryRowDataWithMem(functions.size());
    LOG("init reUsePrevAggValue getArity : "<< reUsePrevAggValue->getArity())
    reUseNewAggValue = BinaryRowData::createBinaryRowDataWithMem(functions.size());
    sharedAccmulators = BinaryRowData::createBinaryRowDataWithMem(accTypes.size());
}

void GroupAggFunction::InitAggFunctions(int &accStartingIndex, int &aggValueIndex)
{
    vector<string> types = handleInputTypes();
    map<int, int> distinctInfoMap = handleDistinctInfo();
    int aggFuncIndex = 0;
    for (const auto& aggCall : description["aggInfoList"]["aggregateCalls"]) {
        string aggTypeStr = aggCall["name"];
        string aggregationFunction = aggCall["aggregationFunction"];
        string aggType = extractAggFunction(aggTypeStr);
        int filterIndex = aggCall["filterArg"];
        // Be careful here. It only deal with one agg per call. This should be fixed !!
        int aggIndex = aggCall["argIndexes"].get<vector<int>>().empty() ? -1
                                                                        : aggCall["argIndexes"].get<vector<int>>()[0];
        string aggDataType = aggIndex == -1 ? "BIGINT" : types[aggIndex];
        AggsHandleFunction* function = nullptr;
        bool shouldDoRetract = false;
        // aggIndex -> column index of the input row (input row from vectorBatch)
        // accIndex -> agg function value index in results row (row from acc state)
        if (aggType == "AVG") {
            function = new AverageFunction(aggIndex, aggDataType, accStartingIndex, accStartingIndex + 1, aggValueIndex,
                                           filterIndex);
        } else if (aggType == "COUNT") {
            if (distinctInfoMap.find(aggFuncIndex) != distinctInfoMap.end()) {
                filterIndex = distinctInfoMap[aggFuncIndex];
                auto *distinctFunction = new CountDistinctFunction(aggIndex, aggDataType, accStartingIndex,
                                                                   aggValueIndex, aggFuncIndex, filterIndex);
                distinctFunction->open(new PerKeyStateDataViewStore(
                        dynamic_cast<StreamingRuntimeContext<RowData *> *>(getRuntimeContext())));
                function = distinctFunction;
            } else {
                function = new CountFunction(aggIndex, aggDataType, accStartingIndex, aggValueIndex, filterIndex);
            }
        } else if (aggType == "MAX") {
            function = new MinMaxFunction(aggIndex, aggDataType, accStartingIndex, aggValueIndex, MAX_FUNC, filterIndex);
        } else if (aggType == "MIN") {
            function = new MinMaxFunction(aggIndex, aggDataType, accStartingIndex, aggValueIndex, MIN_FUNC, filterIndex);
        } else if (aggType == "SUM") {
            shouldDoRetract = (aggregationFunction.find("WithRetract") != std::string::npos) ? true : shouldDoRetract;
            int count0Index = shouldDoRetract ? accStartingIndex + 1 : -1;
            SumFunction *sumFunction = new SumFunction(aggIndex, aggDataType, accStartingIndex,
                                                       aggValueIndex, filterIndex);
            sumFunction->setRetraction(count0Index);
            function = sumFunction;
        } else if (aggType == "last_string_value_without_retract") {
            function = new LastStringValueFunction(aggIndex, aggDataType, accStartingIndex, aggValueIndex);
        } else {
            throw runtime_error("Unsupported aggregate type: " + aggTypeStr);
        }
        // here we only consider the case of one aggregated column for each aggCal.
        functions.push_back(function);
        accStartingIndex += ((aggType == "AVG") || (aggType == "SUM" && shouldDoRetract)) ? 2 : 1;
        aggValueIndex++;
        aggFuncIndex++;
    }
}

JoinedRowData* GroupAggFunction::getResultRow()
{
    return resultRow;
}

void GroupAggFunction::processElement(RowData& input, Context& ctx, TimestampedCollector& out)
{
    bool firstRow;
    bool isEqual = true;
    RowData* currentKey = ctx.getCurrentKey();
    RowData* accumulators = accState->value();

    if (accumulators == nullptr) {
        // This is a new key
        if (!currentKey) {
            LOG("current key is nullptr")
            throw std::runtime_error("current key is nullptr");
        }
        RowData* updatedKey = currentKey->copy();
        ctx.setCurrentKey(updatedKey);
        currentKey = updatedKey;
        if (RowDataUtil::isRetractMsg(input.getRowKind())) {
            return;
        }
        firstRow = true;
        accumulators = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
        auto binRowAcc = static_cast<BinaryRowData*>(accumulators);
        for (int i = 0; i < accumulatorArity; i++) {
            binRowAcc->setNullAt(i);
        }
        // Flink don't do update here, it updates it in if (!recordCounter->recordCountIsZero(accumulators)){} line 146
        static_cast<HeapValueState<RowData*, VoidNamespace, RowData*> *>(accState)->update(accumulators);
    } else {
        firstRow = false;
    }
    // set accumulators to handler first
    for (auto& func : functions) {
        func->setAccumulators(accumulators);
    }
    // get previous aggregate result
    for (int i = 0; i < aggregateCallsCount; ++i) {
        // Fill reUsePrevAggValue with current value
        functions[i]->getValue(reUsePrevAggValue);
        if (RowDataUtil::isAccumulateMsg(input.getRowKind())) {
            functions[i]->accumulate(&input);
        } else {
            functions[i]->retract(&input);
        }
        functions[i]->getValue(reUseNewAggValue);
        functions[i]->getAccumulators(reinterpret_cast<BinaryRowData*>(accumulators));
    }
    
    AssembleResultForElement(accumulators, isEqual, firstRow, currentKey, out);
}

void GroupAggFunction::processBatchColumnar(omnistream::VectorBatch *input, const std::vector<RowInfo> &groupInfo,
                                            RowData *accumulators)
{
    // Separate into accumulate and retract batches
    std::vector<int> accumulateIndices;
    std::vector<int> retractIndices;

    for (const RowInfo& info : groupInfo) {
        if (RowDataUtil::isAccumulateMsg(info.rowKind)) {
            accumulateIndices.push_back(info.rowIndex);
        } else {
            retractIndices.push_back(info.rowIndex);
        }
    }

    // Process all aggregate functions
    for (auto &function : functions) {
        function->getValue(reUsePrevAggValue);

        // Batch accumulate
        if (!accumulateIndices.empty()) {
            function->accumulate(input, accumulateIndices);
        }
        // Batch retract
        if (!retractIndices.empty()) {
            function->retract(input, retractIndices);
        }
        function->getValue(reUseNewAggValue);
        function->getAccumulators(reinterpret_cast<BinaryRowData*>(accumulators));
    }
    static_cast<HeapValueState<RowData*, VoidNamespace, RowData*> *>(accState)->update(accumulators);
}

void GroupAggFunction::processBatch(omnistream::VectorBatch *input, KeyedProcessFunction<RowData *,
                                    RowData *, RowData *>::Context &ctx, TimestampedCollector &out)
{
    auto rowCount = input->GetRowCount();
    if (rowCount < 0) {
        return;
    }
    // Use map to organize all data with the same key.
    std::unordered_map<RowData*, std::vector<RowInfo>> keyToRowIndices;
    LOG("getEntireRow rowCount :" << rowCount)
    FillRowIndices(input, keyToRowIndices, rowCount);
    // List of rows to convert to VectorBatch
    std::vector<RowData*> resultKeys;
    std::vector<RowData*> resultValues;
    std::vector<RowKind> resultRowKinds;
    // Start traversing each key
    for (auto& pair : keyToRowIndices) {
        bool isEqual = true;
        RowData* currentKey = pair.first;
        RowData* copyKey = currentKey->copy();
        ctx.setCurrentKey(copyKey);
        std::vector<RowInfo>& groupInfo = pair.second;
        RowData* accumulators = accState->value();
        bool firstRow = accumulators == nullptr;
        if (firstRow) {
            if (!FirstRowAccumulate(groupInfo, accumulators)) {
                continue;
            }
        } else {
            firstRow = false;
        }
        LOG("will setAccumulators ")
        for (auto& func : functions) {
            func->setAccumulators(accumulators);
        }
        processBatchColumnar(input, groupInfo, accumulators);
        LOG("functions loop aggregateCallsCount end")
        AssembleResultForBatch(accumulators, isEqual, firstRow, currentKey, resultKeys, resultValues, resultRowKinds);
    }
    ClearEnv(input, resultKeys, resultValues, resultRowKinds, out, keyToRowIndices);
    LOG("GroupAggFunction processBatch end")
}

void GroupAggFunction::deleteRowData(vector<RowData *> &rowVector)
{
    for (auto row: rowVector) {
        delete row;
    }
    rowVector.clear();
}

void GroupAggFunction::setInt(omniruntime::vec::VectorBatch* outputBatch,
    int numRows, int colIndex, std::vector<RowData*> vec)
{
    auto* vector = new omniruntime::vec::Vector<int64_t>(numRows);
    for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
        if (vec[rowIndex]->isNullAt(colIndex)) {
            vector->SetNull(rowIndex);
        } else {
            vector->SetValue(rowIndex, *vec[rowIndex]->getInt(colIndex));
        }
    }
    outputBatch->Append(vector);
}

void GroupAggFunction::setLong(omniruntime::vec::VectorBatch* outputBatch,
    int numRows, int colIndex, std::vector<RowData*> vec)
{
    auto* vector = new omniruntime::vec::Vector<int64_t>(numRows);
    for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
        if (vec[rowIndex]->isNullAt(colIndex)) {
            vector->SetNull(rowIndex);
        } else {
            vector->SetValue(rowIndex, *vec[rowIndex]->getLong(colIndex));
        }
    }
    outputBatch->Append(vector);
}

void GroupAggFunction::setString(omniruntime::vec::VectorBatch* outputBatch,
    int numRows, int colIndex, std::vector<RowData*> vec)
{
    auto* vector = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(numRows);
    for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
        if (vec[rowIndex]->isNullAt(colIndex)) {
            vector->SetNull(rowIndex);
        } else {
            std::string_view strView = vec[rowIndex]->getStringView(colIndex);
            vector->SetValue(rowIndex, strView);
        }
    }
    outputBatch->Append(vector);
}

omnistream::VectorBatch* GroupAggFunction::createOutputBatch(std::vector<RowData*> collectedKeys,
    std::vector<RowData*> collectedValues, std::vector<RowKind> rowKinds)
{
    int numColumns = outputTypes.size();
    auto *outputRowType = new std::vector<omniruntime::type::DataTypeId>;
    for (const auto &typeStr : outputTypes) {
        outputRowType->push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    int numRows = collectedKeys.size(); // Number of rows collected
    int keySize = collectedKeys[0]->getArity();
    // Create a new VectorBatch (empty if no rows exist)
    auto* outputBatch = new omnistream::VectorBatch(numRows);
    for (int colIndex = 0; colIndex < numColumns; colIndex++) {
        std::vector<RowData*> vec = colIndex < keySize ? collectedKeys : collectedValues;
        int offset = colIndex < keySize ? 0 : keySize;
        switch (outputRowType->at(colIndex)) {
            case DataTypeId::OMNI_LONG: {
                setLong(outputBatch, numRows, colIndex - offset, vec);
                break;
            }
            case DataTypeId::OMNI_TIMESTAMP: {
                setLong(outputBatch, numRows, colIndex - offset, vec);
                break;
            }
            case DataTypeId::OMNI_INT: {
                setInt(outputBatch, numRows, colIndex - offset, vec);
                break;
            }
            case DataTypeId::OMNI_DOUBLE: {
                setLong(outputBatch, numRows, colIndex - offset, vec);
                break;
            }
            case DataTypeId::OMNI_BOOLEAN: {
                setInt(outputBatch, numRows, colIndex - offset, vec);
                break;
            }
            case DataTypeId::OMNI_VARCHAR: {
                setString(outputBatch, numRows, colIndex - offset, vec);
                break;
            }
            default: {
                LOG("Unsupported column type in inputRow (createOutputBatch). colIndex : "<<colIndex)
                throw std::runtime_error("Unsupported column type in inputRow");
            }
        }
    }

    // Set row kind for all rows (only if there are rows)
    for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
        outputBatch->setRowKind(rowIndex, rowKinds[rowIndex]);
    }
    return outputBatch;
}

std::vector<int32_t> GroupAggFunction::getKeyedTypes(const std::vector<int32_t> keyedIndex,
                                                     const std::vector<std::string> inputTypes)
{
    std::vector<int32_t> keyedTypes;
    for (int32_t index : keyedIndex) {
        if (index >= 0 && index < static_cast<int32_t>(inputTypes.size())) {
            keyedTypes.push_back(LogicalType::flinkTypeToOmniTypeId(inputTypes[index]));
        }
    }
    return keyedTypes;
}

void GroupAggFunction::collectOutputBatch(TimestampedCollector out, omnistream::VectorBatch *outputBatch)
{
    out.collect(outputBatch);
}

void GroupAggFunction::close() {
}

ValueState<RowData *> *GroupAggFunction::getValueState()
{
    return accState;
}

bool GroupAggFunction::FirstRowAccumulate(std::vector<RowInfo>& groupInfo, RowData*& accumulators)
{
    auto entireIter = groupInfo.begin();
    while (entireIter != groupInfo.end()) {
        if (RowDataUtil::isRetractMsg(entireIter->rowKind)) {
            entireIter = groupInfo.erase(entireIter);
        } else {
            break;
        }
    }
    if (groupInfo.empty()) {
        return false;
    }
    accumulators = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
    // If the accumulator does not exist, traverse functions to create the accumulators.
    for (auto& func : functions) {
        func->createAccumulators(dynamic_cast<BinaryRowData *>(accumulators));
    }
    // Flink don't do update here, it updates it in if (!recordCounter->recordCountIsZero(accumulators)){}
    static_cast<HeapValueState<RowData*, VoidNamespace, RowData*> *>(accState)->update(accumulators);
    return true;
}

void GroupAggFunction::ClearEnv(omnistream::VectorBatch *input, std::vector<RowData *> resultKeys,
                                std::vector<RowData *> resultValues, std::vector<RowKind> resultRowKinds,
                                TimestampedCollector &out,
                                std::unordered_map<RowData*, std::vector<RowInfo>> keyToRowIndices)
{
    omniruntime::vec::VectorHelper::FreeVecBatch(input);
    if (!resultKeys.empty()) {
        resultBatch = createOutputBatch(resultKeys, resultValues, resultRowKinds);
        collectOutputBatch(out, resultBatch);

        // clear bundle
        for (auto& pair : keyToRowIndices) {
            delete pair.first;
        }
        keyToRowIndices.clear();
        // clear resultRows
        resultKeys.clear();
        deleteRowData(resultValues);
        resultRowKinds.clear();
    }
}

void GroupAggFunction::AssembleResultForBatch(RowData* accumulators, bool isEqual, bool firstRow, RowData* currentKey,
                                              std::vector<RowData*>& resultKeys, std::vector<RowData*>& resultValues,
                                              std::vector<RowKind>& resultRowKinds)
{
    if (!recordCounter->recordCountIsZero(accumulators)) {
        // Flink update accumulators in state here. But since we directly take the RowData* and updates in getAccumulator, the value in statebackend is already updated!
        if (!firstRow) {
            if (EndAssemble(isEqual)) {
                return;
            }
            if (generateUpdateBefore) {
                resultKeys.push_back(currentKey);
                resultValues.push_back(reUsePrevAggValue->copy());
                resultRowKinds.push_back(RowKind::UPDATE_BEFORE);
            }
            resultKeys.push_back(currentKey);
            resultValues.push_back(reUseNewAggValue->copy());
            resultRowKinds.push_back(RowKind::UPDATE_AFTER);
        } else {
            resultKeys.push_back(currentKey);
            resultValues.push_back(reUseNewAggValue->copy());
            resultRowKinds.push_back(RowKind::INSERT);
        }
    } else {
        if (!firstRow) {
            resultKeys.push_back(currentKey);
            resultValues.push_back(reUsePrevAggValue->copy());
            resultRowKinds.push_back(RowKind::DELETE);
        }
        accState->clear();
    }
}

void GroupAggFunction::FillRowIndices(omnistream::VectorBatch *input,
                                      std::unordered_map<RowData*, std::vector<RowInfo>>& keyToRowIndices, int rowCount)
{
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        auto key = groupByKeySelector->getKey(input, rowIndex);
        RowKind rowKind = input->getRowKind(rowIndex);
        auto it = keyToRowIndices.find(key);
        if (it != keyToRowIndices.end()) {
            it->second.push_back(RowInfo{rowIndex, rowKind});
        } else {
            keyToRowIndices[key] = {RowInfo{rowIndex, rowKind}};
        }
    }
}

void GroupAggFunction::AssembleResultForElement(RowData *accumulators, bool isEqual, bool firstRow, RowData *currentKey,
                                                TimestampedCollector& out)
{
    if (!recordCounter->recordCountIsZero(accumulators)) {
        // Flink update accumulators in state here. But since we directly take the RowData* and updates in getAccumulator, the value in statebackend is already updated!
        if (!firstRow) {
            if (EndAssemble(isEqual)) {
                return;
            }
            if (generateUpdateBefore) {
                resultRow->replace(currentKey, reUsePrevAggValue)->setRowKind(RowKind::UPDATE_BEFORE);
                out.collect(resultRow);
            }

            resultRow->replace(currentKey, reUseNewAggValue)->setRowKind(RowKind::UPDATE_AFTER);
        } else {
            resultRow->replace(currentKey, reUseNewAggValue)->setRowKind(RowKind::INSERT);
        }
        out.collect(resultRow);
    } else {
        if (!firstRow) {
            resultRow->replace(currentKey, reUsePrevAggValue)->setRowKind(RowKind::DELETE);
            out.collect(resultRow);
        }
        accState->clear();
    }
}

bool GroupAggFunction::EndAssemble(bool isEqual)
{
    for (int i = 0; i < aggregateCallsCount; i++) {
        if (!functions[i]->equaliser(reUsePrevAggValue, reUseNewAggValue)) {
            isEqual = false;
            break;
        }
    }
    if (stateRetentionTime <= 0 && isEqual) {
        return true;
    }
    return false;
}
