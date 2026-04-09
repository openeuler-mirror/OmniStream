#ifndef OMNISTREAM_TIMERTYPEINFO_H
#define OMNISTREAM_TIMERTYPEINFO_H

#include "TypeInformation.h"
#include "typeconstants.h"

class TimerTypeInfo : public TypeInformation {
public:
    explicit TimerTypeInfo(TypeInformation* keyTypeInfo_, TypeInformation* namespaceTypeInfo_, Class* keyClazz_, Class* namespaceClazz_);

    ~TimerTypeInfo() override;

    bool isBasicType() const { return false; }

    bool isTupleType() const { return false; }

    int getArity() const { return 0; }

    int getTotalFields() const { return 1; }

    bool isKeyType() { return false; }

    std::string name() override { return keyTypeInfo->name() + " " + namespaceTypeInfo->name(); }

    TypeSerializer* createTypeSerializer() override;

    BackendDataType getBackendId() const override { return BackendDataType::OBJECT_BK; }
private:
    const char* name_ = TYPE_NAME_TIMER_SERIALIZER;
    TypeInformation* keyTypeInfo;
    TypeInformation* namespaceTypeInfo;

    Class* keyClazz;
    Class* namespaceClazz;
};

#endif //OMNISTREAM_TIMERTYPEINFO_H