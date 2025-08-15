/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef JOBINFORMATIONPOD_H
#define JOBINFORMATIONPOD_H

#include <string>
#include <nlohmann/json.hpp>
#include "JobIDPOD.h"

namespace omnistream {

    class JobInformationPOD {
    public:
    JobInformationPOD() = default;
    JobInformationPOD(const JobIDPOD& jobId, const std::string& jobName)
        : jobId(jobId), jobName(jobName), autoWatermarkInterval(0) {}
    JobInformationPOD(const JobIDPOD& jobId, const std::string& jobName, long autoWatermarkInterval)
        : jobId(jobId), jobName(jobName), autoWatermarkInterval(autoWatermarkInterval) {}

    JobInformationPOD(const JobInformationPOD &other)
        : jobId(other.jobId),
          jobName(other.jobName),
          autoWatermarkInterval(other.autoWatermarkInterval) {
    }

    JobInformationPOD(JobInformationPOD &&other) noexcept
        : jobId(std::move(other.jobId)),
          jobName(std::move(other.jobName)),
          autoWatermarkInterval(other.autoWatermarkInterval) {
    }

    bool operator==(const JobInformationPOD& other) const
    {
        return jobId == other.jobId && jobName == other.jobName;
    }

    JobInformationPOD& operator=(const JobInformationPOD &other)
            {
        if (this == &other) {
            return *this;
        }
        jobId = other.jobId;
        jobName = other.jobName;
        autoWatermarkInterval = other.autoWatermarkInterval;
        return *this;
    }

    JobInformationPOD& operator=(JobInformationPOD &&other) noexcept
            {
        if (this == &other) {
            return *this;
        }
        jobId = std::move(other.jobId);
        jobName = std::move(other.jobName);
        autoWatermarkInterval = other.autoWatermarkInterval;
        return *this;
    }

    JobIDPOD getJobId() const { return jobId; }
    void setJobId(const JobIDPOD& jobId) { this->jobId = jobId; }

    std::string getJobName() const { return jobName; }
    void setJobName(const std::string& jobName) { this->jobName = jobName; }

    long getAutoWatermarkInterval() const
    {
        return autoWatermarkInterval;
    }

    void setAutoWatermarkInterval(const long autoWatermarkInterval)
    {
        this->autoWatermarkInterval = autoWatermarkInterval;
    }

    std::string toString() const
    {
        return "JobInformationPOD{ jobId=" + jobId.toString() +
               ", jobName='" + jobName + '\'' +
               ", autoWatermarkInterval'" + std::to_string(autoWatermarkInterval) + '\'' +
               '}';
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(JobInformationPOD, jobId, jobName, autoWatermarkInterval)

    private:
        JobIDPOD jobId;
        std::string jobName;
        long autoWatermarkInterval;
    };

} // namespace omnistream
namespace std {
    template <>
    struct hash<omnistream::JobInformationPOD> {
        std::size_t operator()(const omnistream::JobInformationPOD& obj) const
        {
            std::size_t h1 = hash_value(obj.getJobId());
            std::size_t h2 = std::hash<std::string>{}(obj.getJobName());
            return h1 ^ (h2 << 1);
        }
    };
}

#endif // JOBINFORMATIONPOD_H
