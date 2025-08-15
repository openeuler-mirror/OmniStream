#include "ChainingOutput.h"
#include "../../table/data/RowData.h"
#include "metrics/groups/TaskMetricGroup.h"
#include "operators/AbstractStreamOperator.h"


ChainingOutput::ChainingOutput(Input *op) : operator_(op), announcedStatus(new WatermarkStatus(WatermarkStatus::activeStatus))
{
    // counter_ = new SimpleCounter();
    watermarkGauge = new WatermarkGauge();
}

ChainingOutput::ChainingOutput(Input *op, const std::shared_ptr<omnistream::TaskMetricGroup>& metricGroup,
                               omnistream::OperatorPOD &opConfig)
    : operator_(op), watermarkGauge(new WatermarkGauge()),
      announcedStatus(new WatermarkStatus(WatermarkStatus::activeStatus))
{
    if (metricGroup != nullptr) {
        auto ptr = metricGroup->GetInternalOperatorIOMetric(opConfig.getName(), "numRecordsOut");
        LOG("numRecordsOut add" << reinterpret_cast<long>(ptr.get()))
        numRecordsOut = reinterpret_cast<std::shared_ptr<omnistream::SimpleCounter> &>(ptr);
    } else {
        numRecordsOut = nullptr;
    }
}

void ChainingOutput::collect(void *record)
{
    LOG(" ChainingOutput collect >>>>>>>>")
    LOG("operator address " + std::to_string(reinterpret_cast<long>(operator_)))
    LOG("pass to operator: " + std::string(operator_->getName()))
    LOG("stream record address  " + std::to_string(reinterpret_cast<long>(record)))
    if (numRecordsOut != nullptr) {
        numRecordsOut->Inc(reinterpret_cast<omnistream::VectorBatch*>(
            reinterpret_cast<StreamRecord *>(record)->getValue())->GetRowCount());
    }
    operator_->processBatch(reinterpret_cast<StreamRecord *>(record));
}

void ChainingOutput::close()
{
    // do nothing
}
void ChainingOutput::emitWatermark(Watermark *mark)
{
    LOG("ChainingOutput::emitWatermark: " << mark->getTimestamp() << " name: " << operator_->getName())
    watermarkGauge->setCurrentwatermark(mark->getTimestamp());
    operator_->ProcessWatermark(mark);
}

void ChainingOutput::emitWatermarkStatus(WatermarkStatus *watermarkStatus)
{
    if (!announcedStatus->Equals(watermarkStatus))
    {
        announcedStatus = watermarkStatus;
        operator_->processWatermarkStatus(watermarkStatus);
    }
}
