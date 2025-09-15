/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_THROWINGRUNNABLE_H
#define OMNISTREAM_THROWINGRUNNABLE_H
#include <atomic>
#include  "common.h"
#include <memory>

namespace omnistream
{
    class ThrowingRunnable
    {
    public:
        virtual ~ThrowingRunnable() = default;
        virtual void run() = 0;
        virtual void tryCancel() = 0;
        virtual std::string toString() const = 0;
    };

    template <typename T>
    class MemberFunctionRunnable : public ThrowingRunnable
    {
    public:
        using MemberFunctionPtr = void (T::*)();

        MemberFunctionRunnable(std::shared_ptr<T> obj, MemberFunctionPtr func)
            :MemberFunctionRunnable(obj, func, "MemberFunctionRunnable")
        {
        }

        MemberFunctionRunnable(std::shared_ptr<T> obj, MemberFunctionPtr func, std::string  description)
            : obj_(obj), func_(func), description_(description)
        {
        }

        ~MemberFunctionRunnable() override = default; // Use default destructor

        void run() override
        {
            if (cancelled_.load()) // Use .load() for atomic access
            {
                LOG("runnable has been cancelled");
                return;
            }
            if (obj_ && func_)
            {
                (obj_.get()->*func_)(); // Use .get() to access raw pointer
            }
        }

        void tryCancel() override
        {
            cancelled_.store(true); // Use .store() for atomic assignment
        }

        std::string toString() const override
        {
            return "MemberFunctionRunnable: ( " + description_ + ")";
        }

    private:
        std::shared_ptr<T> obj_;
        MemberFunctionPtr func_;
        std::atomic<bool> cancelled_{false};
        std::string description_;
    };

} // namespace omnistream

#endif // OMNISTREAM_THROWINGRUNNABLE_H

/**
 *
 *
*
// Concrete Runnable implementation using member function pointers
template <typename T>
class MemberFunctionRunnable : public Runnable {
public:
    typedef void (T::*MemberFunctionPtr)();

    MemberFunctionRunnable(T* obj, MemberFunctionPtr func) : obj_(obj), func_(func) {}

    void run() override {
        if (obj_ && func_) {
            (obj_->*func_)();
        }
    }

private:
    T* obj_;
    MemberFunctionPtr func_;
};

// Example class 1
class MyClass1 {
public:
    void doSomething() {
        std::cout << "MyClass1::doSomething() called." << std::endl;
    }
};

// Example class 2
class MyClass2 {
public:
    void performAction() {
        std::cout << "MyClass2::performAction() called." << std::endl;
    }
};

int main() {
    std::queue<std::unique_ptr<Runnable>> taskQueue;

    // Create and enqueue MemberFunctionRunnable objects
    MyClass1 obj1;
    taskQueue.push(std::make_unique<MemberFunctionRunnable<MyClass1>>(&obj1, &MyClass1::doSomething));

    MyClass2 obj2;
    taskQueue.push(std::make_unique<MemberFunctionRunnable<MyClass2>>(&obj2, &MyClass2::performAction));

    // Process the queue
    while (!taskQueue.empty()) {
        std::unique_ptr<Runnable> task = std::move(taskQueue.front()); // Move ownership
        taskQueue.pop();
        task->run();
    }

    return 0;
}
 */
