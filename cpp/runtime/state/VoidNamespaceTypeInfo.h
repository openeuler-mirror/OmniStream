#ifndef OMNISTREAM_VOIDNAMESPACETYPEINFO_H
#define OMNISTREAM_VOIDNAMESPACETYPEINFO_H

#include "core/typeinfo/TypeInformation.h"
#include "core/typeinfo/typeconstants.h"

class VoidNamespaceTypeInfo : public TypeInformation {
public:
    explicit VoidNamespaceTypeInfo(const char* name);

    TypeSerializer* createTypeSerializer() override;

    std::string name() override;

    BackendDataType getBackendId() const override {return BackendDataType::VOID_NAMESPACE_BK;}

private:
    const char* name_ = TYPE_NAME_VOID_NAMESPACE;
};

#endif  // OMNISTREAM_VOIDNAMESPACETYPEINFO_H