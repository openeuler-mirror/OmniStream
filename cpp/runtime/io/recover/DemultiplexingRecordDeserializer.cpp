
#include "DemultiplexingRecordDeserializer.h"
namespace omnistream{
DemultiplexingRecordDeserializer *DemultiplexingRecordDeserializer::UNMAPPED =
   new DemultiplexingRecordDeserializer(
        std::map<long,
                 std::shared_ptr<typename DemultiplexingRecordDeserializer::VirtualChannel>>());
}
//
