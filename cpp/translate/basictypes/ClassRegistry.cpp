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
#include "basictypes/ClassRegistry.h"
ClassRegistry& ClassRegistry::instance()
{
    static ClassRegistry registry;
    return registry;
}

ClassRegistry::~ClassRegistry()
{
    for (auto it = classes_.begin(); it != classes_.end(); ++it) {
        it->second->putRefCount();
    }
    classes_.clear();
}

void ClassRegistry::registerClass(const std::string &name, Class *clazz)
{
    classes_[name] = clazz;
}

Class* ClassRegistry::getClass(const std::string& name)
{
    auto it = classes_.find(name);
    if (it != classes_.end()) {
        return it->second;
    }
    Class* newClass = new Class(name);
    classes_[name] = newClass;
    return newClass;
}

bool ClassRegistry::hasRegistry(const std::string &name)
{
    auto it = classes_.find(name);
    if (it != classes_.end()) {
        return true;
    }
    return false;
}

