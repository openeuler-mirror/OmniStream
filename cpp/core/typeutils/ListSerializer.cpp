/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "ListSerializer.h"
#include "basictypes/java_util_List.h"

ListSerializer::ListSerializer(TypeSerializer* elementSerializer) : elementSerializer(elementSerializer)
{
    setSubBufferReusable(false);
    reuseBuffer = new ArrayList();
}

void ListSerializer::deserialize(Object* buffer, DataInputView &source)
{
    auto list = static_cast<ArrayList *>(buffer);
    int size = source.readInt();
    // create new list with (size + 1) capacity to prevent expensive growth when a single
    // element is added
    if (size == 0) {
        return;
    }
    for (int i = 0; i < size; i++) {
        auto valueBuffer = elementSerializer->GetBuffer();
        elementSerializer->deserialize(valueBuffer, source);
        list->add(valueBuffer);
        valueBuffer->putRefCount();
    }
}

void ListSerializer::serialize(Object* buffer, DataOutputSerializer &target)
{
    auto list = static_cast<ArrayList *>(buffer);
    auto size = list->size();
    target.writeInt(size);
    auto iterator = list->iterator();
    // We iterate here rather than accessing by index, because we cannot be sure that
    // the given list supports RandomAccess.
    // The Iterator should be stack allocated on new JVMs (due to escape analysis)
    while (iterator->hasNext()) {
        auto element = iterator->next();
        elementSerializer->serialize(element, target);
        element->putRefCount();
    }
    iterator->putRefCount();
}

void ListSerializer::setSubBufferReusable(bool bufferReusable_)
{
    elementSerializer->setSelfBufferReusable(bufferReusable_);
}

Object* ListSerializer::GetBuffer()
{
    if (bufferReusable) {
        static_cast<ArrayList *>(reuseBuffer)->clear();
        reuseBuffer->getRefCount();
        return reuseBuffer;
    }
    return new ArrayList();
}