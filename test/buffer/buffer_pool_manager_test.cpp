//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <cstdio>
#include <iostream>
#include <random>
#include <string>

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
// Check whether pages containing terminal characters can be recovered
TEST(BufferPoolManagerTest, DISABLED_BinaryDataTest) {  // DISABLED_BinaryDataTest
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<char> uniform_dist(0);

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  char random_binary_data[BUSTUB_PAGE_SIZE];
  // Generate random binary data
  for (char &i : random_binary_data) {
    i = uniform_dist(rng);
  }

  // Insert terminal characters both in the middle and at end
  random_binary_data[BUSTUB_PAGE_SIZE / 2] = '\0';
  random_binary_data[BUSTUB_PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::memcpy(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE);
  EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} we should be able to create 5 new pages
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
    bpm->FlushPage(i);
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    bpm->UnpinPage(page_id_temp, false);
  }
  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));
  EXPECT_EQ(true, bpm->UnpinPage(0, true));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}
TEST(BufferPoolManagerTest, BpmTest) {  // DISABLED_SampleTest
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 2;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  bpm->NewPageGuarded(&page_id_temp);
  bpm->FetchPage(0);
  bpm->UnpinPage(0, false);
  bpm->FetchPageBasic(0);

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}
// NOLINTNEXTLINE
TEST(BufferPoolManagerTest, DISABLED_SampleTest) {  // DISABLED_SampleTest
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), BUSTUB_PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one buffer page left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
  }
  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
  // now be pinned. Fetching page 0 should fail.
  EXPECT_EQ(true, bpm->UnpinPage(0, true));
  EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  EXPECT_EQ(nullptr, bpm->FetchPage(0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}
TEST(BufferPoolManagerTest, SampleTest2) {  // DISABLED_SampleTest
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  snprintf(page0->GetData(), BUSTUB_PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}
TEST(BufferPoolManagerTest, DISABLED_SampleTest3) {  // DISABLED_SampleTest
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm83 = new BufferPoolManager(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;

  bpm83->NewPage(&page_id_temp);
  bpm83->NewPage(&page_id_temp);
  bpm83->NewPage(&page_id_temp);
  bpm83->NewPage(&page_id_temp);
  bpm83->NewPage(&page_id_temp);
  bpm83->NewPage(&page_id_temp);
  bpm83->NewPage(&page_id_temp);
  bpm83->NewPage(&page_id_temp);
  bpm83->NewPage(&page_id_temp);
  bpm83->NewPage(&page_id_temp);
  bpm83->FetchPage(0);
  bpm83->UnpinPage(0, true);
  bpm83->UnpinPage(0, true);
  bpm83->FlushPage(0);
  bpm83->FetchPage(1);
  bpm83->UnpinPage(1, true);
  bpm83->UnpinPage(1, true);
  bpm83->FlushPage(1);
  bpm83->FetchPage(2);
  bpm83->UnpinPage(2, true);
  bpm83->UnpinPage(2, true);
  bpm83->FlushPage(2);
  bpm83->FetchPage(3);
  bpm83->UnpinPage(3, true);
  bpm83->UnpinPage(3, true);
  bpm83->FlushPage(3);
  bpm83->FetchPage(4);
  bpm83->UnpinPage(4, true);
  bpm83->UnpinPage(4, true);
  bpm83->FlushPage(4);
  bpm83->FetchPage(5);
  bpm83->UnpinPage(5, true);
  bpm83->UnpinPage(5, true);
  bpm83->FlushPage(5);
  bpm83->FetchPage(6);
  bpm83->UnpinPage(6, true);
  bpm83->UnpinPage(6, true);
  bpm83->FlushPage(6);
  bpm83->FetchPage(7);
  bpm83->UnpinPage(7, true);
  bpm83->UnpinPage(7, true);
  bpm83->FlushPage(7);
  bpm83->FetchPage(8);
  bpm83->UnpinPage(8, true);
  bpm83->UnpinPage(8, true);
  bpm83->FlushPage(8);
  bpm83->FetchPage(9);
  bpm83->UnpinPage(9, true);
  bpm83->UnpinPage(9, true);
  bpm83->FlushPage(9);
  bpm83->NewPage(&page_id_temp);
  bpm83->UnpinPage(10, true);
  bpm83->NewPage(&page_id_temp);
  bpm83->UnpinPage(11, true);
  bpm83->NewPage(&page_id_temp);
  bpm83->UnpinPage(12, true);
  bpm83->NewPage(&page_id_temp);
  bpm83->UnpinPage(13, true);
  bpm83->NewPage(&page_id_temp);
  bpm83->UnpinPage(14, true);
  bpm83->NewPage(&page_id_temp);
  bpm83->UnpinPage(15, true);
  bpm83->NewPage(&page_id_temp);
  bpm83->UnpinPage(16, true);
  bpm83->NewPage(&page_id_temp);
  bpm83->UnpinPage(17, true);
  bpm83->NewPage(&page_id_temp);
  bpm83->UnpinPage(18, true);
  bpm83->NewPage(&page_id_temp);
  bpm83->UnpinPage(19, true);
  bpm83->FetchPage(0);
  bpm83->FetchPage(1);
  bpm83->FetchPage(2);
  bpm83->FetchPage(3);
  bpm83->FetchPage(4);
  bpm83->FetchPage(5);
  bpm83->FetchPage(6);
  bpm83->FetchPage(7);
  bpm83->FetchPage(8);
  bpm83->FetchPage(9);
  bpm83->UnpinPage(4, true);
  bpm83->NewPage(&page_id_temp);
  bpm83->FetchPage(4);
  bpm83->FetchPage(5);
  bpm83->FetchPage(6);
  bpm83->FetchPage(7);
  bpm83->UnpinPage(5, false);
  bpm83->UnpinPage(6, false);
  bpm83->UnpinPage(7, false);
  bpm83->UnpinPage(5, false);
  bpm83->UnpinPage(6, false);
  bpm83->UnpinPage(7, false);
  // EXPECT_EQ(6, page_id_temp);
  bpm83->NewPage(&page_id_temp);
  bpm83->FetchPage(5);
  bpm83->FetchPage(7);
  EXPECT_EQ(nullptr, bpm83->FetchPage(6));
  bpm83->UnpinPage(21, false);
  bpm83->FetchPage(6);
  EXPECT_EQ(nullptr, bpm83->NewPage(&page_id_temp));

  disk_manager->ShutDown();
  remove("test.db");

  delete bpm83;
  delete disk_manager;
}
}  // namespace bustub
