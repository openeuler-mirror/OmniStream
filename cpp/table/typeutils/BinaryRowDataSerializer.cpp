#include "BinaryRowDataSerializer.h"
// #include "core/memory/MemorySegmentFactory.h"
#include "../data/binary/BinarySegmentUtils.h"

BinaryRowDataSerializer::BinaryRowDataSerializer(int numFields) : numFields_(numFields)
{
    fixedLengthPartSize_ = BinaryRowData::calculateFixPartSizeInBytes(numFields_);
    reUse_ = new BinaryRowData(numFields_);
    // numSegment_ = 1;
    auto *bytes = new uint8_t[SEG_SIZE];
    // MemorySegment *firstSeg = MemorySegmentFactory::wrap(bytes, SEG_SIZE);
    // backData_ = new MemorySegment *[numSegment_];
    // backData_[0] = firstSeg;
    reUse_->pointTo(bytes, 0, SEG_SIZE, SEG_SIZE);
}

BinaryRowDataSerializer::~BinaryRowDataSerializer()
{
    delete reUse_;
}

void *BinaryRowDataSerializer::deserialize(DataInputView &source)
{
    int length = source.readInt();

    // TBD assume we always use one segment
    // auto bytes = backData_[0]->getAll();
    auto bytes = reUse_->getSegment();
    LOG(" bytes: " << reinterpret_cast<long>(bytes)<< " capacity: " << length << " offset : " << 0 << " length: " << length)
    if (length > SEG_SIZE) {
        LOG("Warning! Deserialize bytes length is " << length << ". Bigger than " << SEG_SIZE)
    }
    source.readFully(bytes, length, 0, length);
    // PRINT_HEX(bytes, 0, length)
    // reUse_->pointTo(backData_, numSegment_, 0, length);
    reUse_->setSizeInBytes(length);

    return reUse_;
}


void BinaryRowDataSerializer::serialize(void *row, DataOutputSerializer &target) {
    LOG(">>>>")
    // TODO: This is just a temporary solution to pass single Agg/Join op test
    // We eventually needs to implement a JoinedRowDataSerializer
    // in Q4, Agg/Join are never the last Op in chain, so JoinedRowDataSerialzier is not needed of Q4.
    auto *castedRow = reinterpret_cast<RowData *>(row);
    if (castedRow->getRowDataTypeId() == RowData::JoinedRowDataID)
    {
        INFO_RELEASE("WARNING: joinedRowToBinaryRow are only meant for testing. It only treat long type!!");
        reUse_ = joinedRowToBinaryRow(static_cast<JoinedRowData *>(row));
    }
    else
    {
        auto *binRow = reinterpret_cast<BinaryRowData *>(row);
        LOG(">>>>" << binRow->getSizeInBytes())
        target.writeInt(binRow->getSizeInBytes());

        LOG("getBufferCapacity>>>>" << binRow->getBufferCapacity() << " , getOffset :" << binRow->getOffset())

        BinarySegmentUtils::copyToView(binRow->getSegment(),
                                       binRow->getBufferCapacity(), binRow->getOffset(),
                                       binRow->getSizeInBytes(), target);
        // BinarySegmentUtils::copyToView(binRow->getSegments(),
        //                                binRow->getNumSegments(), binRow->getOffset(),
        //                                binRow->getSizeInBytes(), target);
    }
}

const char *BinaryRowDataSerializer::getName() const
{
    return "BinaryRowDataSerializer";
}

BinaryRowData* BinaryRowDataSerializer::joinedRowToBinaryRow(JoinedRowData *row, const std::vector<int32_t>& typeId)
{
    if (typeId.empty()) {
        INFO_RELEASE("WARNING: joinedRowToBinaryRow are only meant for testing. It only treat long type!!");
    }
    // by default, all fields are long or timestamp. If there are other type, typeId need to be provided.
    // Eventually, we will have a JoinedRowDataSerializer. This is just for testing purposes
    BinaryRowData *newRow = BinaryRowData::createBinaryRowDataWithMem(row->getArity());
    newRow->setRowKind(row->getRowKind());
    if (!typeId.empty()) {
        assert(typeId.size() == static_cast<size_t>(row->getArity()));
    }
    for (int i = 0; i < newRow->getArity(); i++)
    {
        if (typeId.empty() || typeId[i] == omniruntime::type::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE ||
            typeId[i] == omniruntime::type::OMNI_LONG || typeId[i] == omniruntime::type::OMNI_TIMESTAMP ||
            typeId[i] == omniruntime::type::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            newRow->setLong(i, *row->getLong(i));
        } else if (!typeId.empty() &&
            (typeId[i] == omniruntime::type::OMNI_VARCHAR || typeId[i] == omniruntime::type::OMNI_CHAR)) {
            newRow->setStringView(i, row->getStringView(i));
        } else {
            throw std::runtime_error("type not supported!");
        }
    }
    return newRow;
}

BinaryRowData* BinaryRowDataSerializer::joinedRowFromBothBinaryRow(JoinedRowData *row)
{
    // Both  row1 and row2  are of type BinaryRowData
    if (!dynamic_cast<BinaryRowData *>(row->getRow1())) {
        throw std::runtime_error("row1 is not BinaryRowData type");
    }
    if (!dynamic_cast<BinaryRowData *>(row->getRow2())) {
        throw std::runtime_error("row2 is not BinaryRowData type");
    }
    auto *row1 = dynamic_cast<BinaryRowData *>(row->getRow1());
    auto *row2 = dynamic_cast<BinaryRowData *>(row->getRow2());
    BinaryRowData *newRow = BinaryRowData::createRowFromSubJoinedRows(row1,row2);
    newRow->setRowKind(row->getRowKind());
    return newRow;
}


