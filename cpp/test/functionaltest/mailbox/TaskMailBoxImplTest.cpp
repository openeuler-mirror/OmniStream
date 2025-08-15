//
// Created by root on 3/20/25.
//
#include <gtest/gtest.h>
#include <stdexcept>
#include <atomic>
#include <thread>
#include <future>
#include <stdexcept>
#include <memory>
#include "runtime/tasks/mailbox/MailboxProcessor.h"
#include "runtime/tasks/mailbox/MailboxExecutor.h"
#include "runtime/tasks/mailbox/MailboxDefaultAction.h"
#include <exception>
#include "runtime/tasks/mailbox/MailboxController.h"
#include "runtime/tasks/mailbox/TaskMailbox.h"
#include "runtime/tasks/mailbox/Mail.h"
#include "runtime/tasks/mailbox/TaskMailboxImpl.h"
#include "core/utils/function/ThrowingRunnable.h"

#include <atomic>
#include <vector>
#include <string>
#include <queue>


//this class is used for mail contructor
class NoOpRunnable : public omnistream::ThrowingRunnable {
  public:
      void run() override {}

      void tryCancel() override {}

      std::string toString() const override {
          return "NoOpRunnable: No operation.";
      }
};

//testPutAsHead
int MAX_PRIORITY = 1;
int DEFAULT_PRIORITY = 0;

//
TEST(TaskMailboxImplTest, testhasMailFunction){

    std::thread::id mailboxThreadId = std::this_thread::get_id();  // get current thread ID

    std::shared_ptr<omnistream::TaskMailbox> taskMailbox = std::make_shared<omnistream::TaskMailboxImpl>(mailboxThreadId);

    std::cout<<"mailboxThreadId"<<mailboxThreadId<<std::endl;
    auto mailA=std::make_shared<omnistream::Mail>(
      std::make_shared<NoOpRunnable>(),
      MAX_PRIORITY,
      "mailA",
      std::vector<std::string>{});

    taskMailbox->put(mailA);

    ASSERT_EQ(taskMailbox->hasMail(),true);
}


TEST(TaskMailboxImplTest, testhasputhead){
  std::thread::id mailboxThreadId = std::this_thread::get_id();  // get current thread ID

  std::shared_ptr<omnistream::TaskMailbox> taskMailbox = std::make_shared<omnistream::TaskMailboxImpl>(mailboxThreadId);

  std::cout<<"mailboxThreadId"<<mailboxThreadId<<std::endl;
    auto mailA=std::make_shared<omnistream::Mail>(
      std::make_shared<NoOpRunnable>(),
      MAX_PRIORITY,
      "mailA",
      std::vector<std::string>{});

    auto mailB=std::make_shared<omnistream::Mail>(
      std::make_shared<NoOpRunnable>(),
      MAX_PRIORITY,
      "mailB",
      std::vector<std::string>{});

    auto mailC=std::make_shared<omnistream::Mail>(
      std::make_shared<NoOpRunnable>(),
      DEFAULT_PRIORITY,
      "mailC",
      std::vector<std::string>{});

    auto mailD=std::make_shared<omnistream::Mail>(
      std::make_shared<NoOpRunnable>(),
      DEFAULT_PRIORITY,
      "mailD",
      std::vector<std::string>{});

    taskMailbox->put(mailC);
    taskMailbox->putFirst(mailB);
    taskMailbox->put(mailD);
    taskMailbox->putFirst(mailA);

    ASSERT_EQ(taskMailbox->take(MAX_PRIORITY), mailA);
    ASSERT_EQ(taskMailbox->take(DEFAULT_PRIORITY), mailB);
    ASSERT_EQ(taskMailbox->take(DEFAULT_PRIORITY), mailC);
    ASSERT_EQ(taskMailbox->take(DEFAULT_PRIORITY), mailD);

}


// testContracts
TEST(TaskMailboxImplTest,testContracts){
  int MAX_PRIORITY = 1;
  int DEFAULT_PRIORITY = 0;
  std::queue<std::shared_ptr<omnistream::Mail>> testObjects;
  std::thread::id mailboxThreadId = std::this_thread::get_id();  // get current thread ID

  std::shared_ptr<omnistream::TaskMailbox> taskMailbox = std::make_shared<omnistream::TaskMailboxImpl>(mailboxThreadId);

  ASSERT_FALSE(taskMailbox->hasMail());

  for(int i = 0; i < 10; i++){
    std::shared_ptr<omnistream::Mail> mail = std::make_shared<omnistream::Mail>(
      std::make_shared<NoOpRunnable>(), DEFAULT_PRIORITY, "mail, DEFAULT_PRIORITY", std::vector<std::string>{}
    );
    testObjects.push(mail);
    taskMailbox->put(mail);
    ASSERT_TRUE(taskMailbox->hasMail());

  }

  while(testObjects.empty()==false){
    ASSERT_EQ(taskMailbox->take(DEFAULT_PRIORITY),testObjects.front());
    testObjects.pop();
    ASSERT_EQ(taskMailbox->hasMail(),!testObjects.empty());
  }
}


void testLifecyclePuttingInternal(std::shared_ptr<omnistream::TaskMailbox> taskMailbox){
  try {
    auto mail=std::make_shared<omnistream::Mail>(
      std::make_shared<NoOpRunnable>(),
      DEFAULT_PRIORITY,
      "mail",
      std::vector<std::string>{});
    taskMailbox->put(mail);
    FAIL() << "Expected MailboxClosedException";
  } catch (...) {
    SUCCEED();
  }
}

// // //testLifeCycleQuiesce
TEST(TaskMailboxImplTest, testLifeCycleQuiesce) {
  std::thread::id mailboxThreadId = std::this_thread::get_id();  // get current thread ID

  std::shared_ptr<omnistream::TaskMailbox> taskMailbox = std::make_shared<omnistream::TaskMailboxImpl>(mailboxThreadId);

  auto mailA=std::make_shared<omnistream::Mail>(
    std::make_shared<NoOpRunnable>(),
    DEFAULT_PRIORITY,
    "mailA",
    std::vector<std::string>{});

  auto mailB=std::make_shared<omnistream::Mail>(
    std::make_shared<NoOpRunnable>(),
    DEFAULT_PRIORITY,
    "mailB",
    std::vector<std::string>{});


  taskMailbox->put(mailA);  // add two Mail
  taskMailbox->put(mailB);
  taskMailbox->quiesce();  //make the box quiesce
  testLifecyclePuttingInternal(taskMailbox);
  taskMailbox->take(DEFAULT_PRIORITY);  // remove one task

  // check tryTake
  auto mail = taskMailbox->tryTake(DEFAULT_PRIORITY);
  bool result=mail.has_value();
  ASSERT_TRUE(result);  // should have one in box
  auto mail2 = taskMailbox->tryTake(DEFAULT_PRIORITY);
  bool result2= mail2.has_value();
  ASSERT_FALSE(result2);
}


TEST(TaskMailboxImplTest, testLifeCycleClose) {
  std::thread::id mailboxThreadId = std::this_thread::get_id();  // get current thread ID

  std::shared_ptr<omnistream::TaskMailbox> taskMailbox = std::make_shared<omnistream::TaskMailboxImpl>(mailboxThreadId);
  taskMailbox->close(); //close the mailbox
  testLifecyclePuttingInternal(taskMailbox); //should throw exception because the mailbox is closed
  ASSERT_THROW(taskMailbox->take(DEFAULT_PRIORITY), omnistream::TaskMailbox::MailboxClosedException); //take one mail from box, should throw exception
  ASSERT_THROW(taskMailbox->tryTake(DEFAULT_PRIORITY), omnistream::TaskMailbox::MailboxClosedException); // try to take one mail from box, should throw exception
}


TEST(TaskMailboxImplTest, testPutAsHeadWithPriority){
  std::thread::id mailboxThreadId = std::this_thread::get_id();  // get current thread ID

  std::shared_ptr<omnistream::TaskMailbox> taskMailbox = std::make_shared<omnistream::TaskMailboxImpl>(mailboxThreadId);
  auto mailA=std::make_shared<omnistream::Mail>(
    std::make_shared<NoOpRunnable>(),
    2,
    "mailA",
    std::vector<std::string>{});
  auto mailB=std::make_shared<omnistream::Mail>(
    std::make_shared<NoOpRunnable>(),
    2,
    "mailB",
    std::vector<std::string>{});
  auto mailC=std::make_shared<omnistream::Mail>(
    std::make_shared<NoOpRunnable>(),
    1,
    "mailC",
    std::vector<std::string>{});
  auto mailD=std::make_shared<omnistream::Mail>(
    std::make_shared<NoOpRunnable>(),
    1,
    "mailD",
    std::vector<std::string>{});


    taskMailbox->put(mailC);
    taskMailbox->put(mailB);
    taskMailbox->put(mailD);
    taskMailbox->putFirst(mailA);

  ASSERT_EQ(taskMailbox->take(DEFAULT_PRIORITY),mailA);
  ASSERT_EQ(taskMailbox->take(DEFAULT_PRIORITY),mailC);
  ASSERT_EQ(taskMailbox->take(DEFAULT_PRIORITY),mailB);
  ASSERT_EQ(taskMailbox->take(DEFAULT_PRIORITY),mailD);

}

TEST(TaskMailboxImplTest,testBatchAndNonBatchTake){
  std::thread::id mailboxThreadId = std::this_thread::get_id();  // get current thread ID

  std::shared_ptr<omnistream::TaskMailbox> taskMailbox = std::make_shared<omnistream::TaskMailboxImpl>(mailboxThreadId);
  std::vector<std::shared_ptr<omnistream::Mail>>mails;

  //make a vector containing 6 mail tasks
  for(int i=0;i<6;i++){
    auto mail=std::make_shared<omnistream::Mail>(
      std::make_shared<NoOpRunnable>(),
      1,
      std::to_string(i),
      std::vector<std::string>{}
    );
    mails.push_back(mail);
  }

  for(int i=0;i<3;i++){
    taskMailbox->put(mails[i]);
  }

  bool result=taskMailbox->createBatch();
  ASSERT_EQ(result,true);
  for(int i=3;i<6;i++){
    taskMailbox->put(mails[i]);
  }
  // // now take all mails in the batch with all available methods
  ASSERT_EQ(taskMailbox->tryTakeFromBatch(),mails[0]);
  ASSERT_EQ(taskMailbox->tryTake(1),mails[1]);
  ASSERT_EQ(taskMailbox->take(1),mails[2]);
  // // batch empty, so only regular methods work
  ASSERT_FALSE(taskMailbox->tryTakeFromBatch().has_value());


  ASSERT_EQ(taskMailbox->tryTake(1),mails[3]);
  ASSERT_EQ(taskMailbox->tryTake(1),mails[4]);
  //check the last mail in box
  auto closedMails = taskMailbox->close();
  ASSERT_EQ(closedMails.size(), 1);
  ASSERT_EQ(closedMails[0], mails[5]);
}

TEST(TaskMailboxImplTest,testBatchDrain){
  std::thread::id mailboxThreadId = std::this_thread::get_id();  // get current thread ID

  std::shared_ptr<omnistream::TaskMailbox> taskMailbox = std::make_shared<omnistream::TaskMailboxImpl>(mailboxThreadId);
  auto mailA=std::make_shared<omnistream::Mail>(
    std::make_shared<NoOpRunnable>(),
    1,
    "mailA",
    std::vector<std::string>{});

  auto mailB=std::make_shared<omnistream::Mail>(
    std::make_shared<NoOpRunnable>(),
    2,
    "mailB",
    std::vector<std::string>{});

  taskMailbox->put(mailA);
  bool result=taskMailbox->createBatch();
  ASSERT_TRUE(result);
  taskMailbox->put(mailB);

  //verify that the vector has mailA and mailB
  auto drainedMails = taskMailbox->drain();
  ASSERT_EQ(drainedMails.size(), 2);
  bool findA=false;
  bool findB=false;
  for(int i=0;i<drainedMails.size();i++){
    if (drainedMails[i] == mailA) {
      findA = true;
    }
    if (drainedMails[i] == mailB) {
      findB = true;
    }

  }
  ASSERT_TRUE(findA);
  ASSERT_TRUE(findB);
}

TEST(TaskMailboxImplTest,testBatchPriority){
  std::thread::id mailboxThreadId = std::this_thread::get_id();  // get current thread ID
  std::shared_ptr<omnistream::TaskMailbox> taskMailbox = std::make_shared<omnistream::TaskMailboxImpl>(mailboxThreadId);
  auto mailA=std::make_shared<omnistream::Mail>(
    std::make_shared<NoOpRunnable>(),
    1,
    "mailA",
    std::vector<std::string>{});

  auto mailB=std::make_shared<omnistream::Mail>(
    std::make_shared<NoOpRunnable>(),
    2,
    "mailB",
    std::vector<std::string>{});

  taskMailbox->put(mailA);
  bool result=taskMailbox->createBatch();
  ASSERT_TRUE(result);
  taskMailbox->put(mailB);

  auto mail1=taskMailbox->take(2);
  ASSERT_EQ(mail1,mailB);

  auto mail2=taskMailbox->tryTakeFromBatch();
  ASSERT_TRUE(mail2.has_value());


  ASSERT_EQ(mail2.value(),mailA);

}
// //TODO
// TEST(TaskMailboxImplTest,testRunExclusively){


// }












