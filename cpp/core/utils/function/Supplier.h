/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_SUPPLIER_H
#define OMNISTREAM_SUPPLIER_H

#include <functional>
#include <memory>
#include <string>

namespace omnistream {

    template <typename T>
    class Supplier {
    public:
        virtual ~Supplier() = default;
        virtual std::shared_ptr<T> get() = 0;
        virtual std::string toString() const = 0;
    };


    template <typename T>
    class LambdaSupplier : public Supplier<T> {
    public:
        using SupplierFunction = std::function<std::shared_ptr<T>()>;
        explicit LambdaSupplier(SupplierFunction func) : func_(func) {}
        ~LambdaSupplier() override = default;

        std::shared_ptr<T> get() override {
            return func_();
        }

        std::string toString() const override {
            return "LambdaSupplier";
        }

    private:
        SupplierFunction func_;
    };

} // namespace omnistream

#endif // OMNISTREAM_SUPPLIER_H