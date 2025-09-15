#ifndef OMNISTREAM_TEMP_BATCH_FUNCTION_H
#define OMNISTREAM_TEMP_BATCH_FUNCTION_H
#include <ctime>
#include <iomanip>
#include <sstream>
#include <regex>
#include <string>
#include "VectorBatch.h"
#include "core/include/common.h"
using namespace omniruntime::vec;
namespace omnistream {
    // for DATE_FORMAT
    BaseVector * BatchDateFormat(BaseVector* inputVec, const std::string &format);
    BaseVector* RegexpExtract(BaseVector* inputVec, std::string& regexToMatch, int32_t group);
}


#endif //OMNISTREAM_TEMP_BATCH_FUNCTION_H
