/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: Tuesday December 18th 2018
 * Author: hzsunjianliang
 */

#ifndef SRC_MDS_NAMESERVER2_CLEAN_TASK_H_
#define SRC_MDS_NAMESERVER2_CLEAN_TASK_H_

#include <functional>
#include <memory>  //NOLINT
#include <mutex>  //NOLINT
#include <map>
#include <unordered_set>
#include <condition_variable>
#include "src/common/concurrent/concurrent.h"
#include <brpc/closure_guard.h>  //NOLINT
#include <brpc/controller.h>    //NOLINT
#include "proto/nameserver2.pb.h"
#include "src/mds/nameserver2/task_progress.h"
#include "src/mds/nameserver2/clean_core.h"
#include "src/mds/nameserver2/async_delete_snapshot_entity.h"

namespace curve {
namespace mds {

typedef  uint64_t TaskIDType;

// default clean task retry times
const uint32_t kDefaultTaskRetryTimes = 5;

class Task {
 public:
    Task() : taskID_(0), progress_(), retry_(1) {}

    virtual void Run(void) = 0;

    std::function<void()> Closure() {
        return [this] () {
            Run();
        };
    }

    TaskProgress GetTaskProgress(void) const {
        return progress_;
    }

    void SetTaskProgress(TaskProgress progress) {
        progress_ = progress;
    }

    TaskProgress* GetMutableTaskProgress(void) {
        return &progress_;
    }

    void SetTaskID(TaskIDType taskID) {
        taskID_ = taskID;
    }

    TaskIDType GetTaskID(void) const {
        return taskID_;
    }

    void SetRetryTimes(uint32_t retry) {
        retry_ = retry;
    }

    void Retry() {
        retry_--;
        progress_ = TaskProgress();
    }

    bool RetryTimesExceed() {
        return retry_ == 0;
    }

 protected:
    TaskIDType taskID_;
    TaskProgress progress_;
    // 任务最大重试次数
    uint32_t retry_;
};

class SnapShotCleanTask: public Task {
 public:
    SnapShotCleanTask(TaskIDType taskID, std::shared_ptr<CleanCore> core,
                FileInfo fileInfo,
                std::shared_ptr<AsyncDeleteSnapShotEntity> entity = nullptr) {
        cleanCore_ = core;
        fileInfo_ = fileInfo;
        SetTaskProgress(TaskProgress());
        SetTaskID(taskID);
        asyncEntity_ = entity;
        SetRetryTimes(kDefaultTaskRetryTimes);
    }
    void Run(void) override {
        StatusCode ret = cleanCore_->CleanSnapShotFile(fileInfo_,
                                                    GetMutableTaskProgress());
        if (asyncEntity_ != nullptr) {
            brpc::ClosureGuard doneGuard(asyncEntity_->GetClosure());
            brpc::Controller* cntl =
                static_cast<brpc::Controller*>(asyncEntity_->GetController());
            DeleteSnapShotResponse *response =
                        asyncEntity_->GetDeleteResponse();
            const DeleteSnapShotRequest  *request  =
                        asyncEntity_->GetDeleteRequest();

            response->set_statuscode(ret);
            if (ret != StatusCode::kOK) {
                LOG(ERROR) << "logid = " << cntl->log_id()
                    << ", CleanSnapShotFile fail, filename = "
                    << request->filename()
                    << ", sequencenum = " << request->seq()
                    << ", statusCode = " << ret;
            } else {
                LOG(INFO) << "logid = " << cntl->log_id()
                    << ", CleanSnapShotFile ok, filename = "
                    <<  request->filename()
                    << ", sequencenum = " << request->seq();
            }
        }
        return;
    }

 private:
    std::shared_ptr<CleanCore> cleanCore_;
    FileInfo fileInfo_;
    std::shared_ptr<AsyncDeleteSnapShotEntity> asyncEntity_;
};

class SnapShotCleanTask2: public Task {
 public:
    SnapShotCleanTask2(TaskIDType taskID, std::shared_ptr<CleanCore> core,
                FileInfo fileInfo,
                std::shared_ptr<AsyncDeleteSnapShotEntity> entity = nullptr,
                uint32_t mdsSessionTimeUs = 3000*1000) {
        cleanCore_ = core;
        fileInfo_ = fileInfo;
        SetTaskProgress(TaskProgress());
        SetTaskID(taskID);
        asyncEntity_ = entity;
        SetRetryTimes(kDefaultTaskRetryTimes);
        mdsSessionTimeUs_ = mdsSessionTimeUs;
    }
    void Run(void) override {
        // Wait 2*mdsSessionTime to ensure all clients synchronize with the sequence num,
        // or else the previous chunk request after this snapshot deletion request may result 
        // remaining COW chunk.
        {
            std::unique_lock<common::Mutex> lk(cvMutex_);
            auto now = std::chrono::system_clock::now();
            if (cv_.wait_until(lk, now + std::chrono::microseconds(mdsSessionTimeUs_ * 2), 
                        [&](){return taskCanRun_;})) {
                LOG(INFO) << "SnapShotCleanTask2 filename " << fileInfo_.filename()
                        << " finished waiting";       
            } else {
                LOG(WARNING) << "SnapShotCleanTask2 filename " << fileInfo_.filename()
                        << " wait timeout " << mdsSessionTimeUs_ * 2 << " us."; 
            }
        }

        StatusCode ret = cleanCore_->CleanSnapShotFile2(fileInfo_,
                                                    GetMutableTaskProgress());
        if (asyncEntity_ != nullptr) {
            brpc::ClosureGuard doneGuard(asyncEntity_->GetClosure());
            brpc::Controller* cntl =
                static_cast<brpc::Controller*>(asyncEntity_->GetController());
            DeleteSnapShotResponse *response =
                        asyncEntity_->GetDeleteResponse();
            const DeleteSnapShotRequest  *request  =
                        asyncEntity_->GetDeleteRequest();

            response->set_statuscode(ret);
            if (ret != StatusCode::kOK) {
                LOG(ERROR) << "logid = " << cntl->log_id()
                    << ", CleanSnapShotFile fail, filename = "
                    << request->filename()
                    << ", sequencenum = " << request->seq()
                    << ", statusCode = " << ret;
            } else {
                LOG(INFO) << "logid = " << cntl->log_id()
                    << ", CleanSnapShotFile ok, filename = "
                    <<  request->filename()
                    << ", sequencenum = " << request->seq();
            }
        }
        return;
    }

    /**
     * brief: Start timer to ensure the snapshot deletion task begins running after
     *        all clients are updated with the latest snap context
    */
    void StartTimer() {
        std::thread t([&]() {
            std::this_thread::sleep_for(
                std::chrono::microseconds(mdsSessionTimeUs_ * 2));
            common::LockGuard lck(cvMutex_);
            taskCanRun_ = true;
            cv_.notify_all();
        });
        t.detach();
    }

    FileInfo* GetMutableFileInfo() {
        return &fileInfo_;
    }

 private:
    std::shared_ptr<CleanCore> cleanCore_;
    FileInfo fileInfo_;
    std::shared_ptr<AsyncDeleteSnapShotEntity> asyncEntity_;
    uint32_t mdsSessionTimeUs_;
    bool taskCanRun_ = false;
    common::ConditionVariable cv_;
    common::Mutex cvMutex_;
};

class SnapShotBatchCleanTask: public Task {
 public:
    SnapShotBatchCleanTask(TaskIDType taskID, std::shared_ptr<CleanCore> core,
                std::shared_ptr<NameServerStorage> storage,
                std::shared_ptr<AsyncDeleteSnapShotEntity> entity = nullptr,
                uint32_t mdsSessionTimeUs = 3000*1000) {
        cleanCore_ = core;
        SetTaskProgress(TaskProgress());
        SetTaskID(taskID);
        asyncEntity_ = entity;
        SetRetryTimes(kDefaultTaskRetryTimes);
        mdsSessionTimeUs_ = mdsSessionTimeUs;
        storage_ = storage;
    }

    void Run(void) override;

    bool PushTask(const FileInfo &snapfileInfo);

    std::shared_ptr<Task> GetTask(SeqNum sn);

    bool IsEmpty();

 private:
    std::shared_ptr<SnapShotCleanTask2> front();

    void pop(SeqNum sn);

    /**
     *  @brief Update existing snapshot with sn less than the snapshot to be 
     *         deleted within the clone file where snapshot belongs to.
     *  @param snapshotInfo: snapshot to be deleted
     *  @return result
     */
    bool setCurrentExistingSnaps(FileInfo* snapshotInfo);

 private:
    std::shared_ptr<CleanCore> cleanCore_;
    std::shared_ptr<AsyncDeleteSnapShotEntity> asyncEntity_;
    uint32_t mdsSessionTimeUs_;
    // snapshot clean task ordered by its snap seqnum
    std::map<SeqNum, std::shared_ptr<SnapShotCleanTask2>> cleanOrderedSnapTasks_;
    common::Mutex mutexSnapTask_;
    std::shared_ptr<NameServerStorage> storage_;
};

class CommonFileCleanTask: public Task {
 public:
    CommonFileCleanTask(TaskIDType taskID, std::shared_ptr<CleanCore> core,
                FileInfo fileInfo) {
        cleanCore_ = core;
        fileInfo_ = fileInfo;
        SetTaskProgress(TaskProgress());
        SetTaskID(taskID);
        SetRetryTimes(kDefaultTaskRetryTimes);
    }

    void Run(void) override {
        cleanCore_->CleanFile(fileInfo_, GetMutableTaskProgress());
        return;
    }

 private:
    std::shared_ptr<CleanCore> cleanCore_;
    FileInfo fileInfo_;
};

}  // namespace mds
}  // namespace curve
#endif      //  SRC_MDS_NAMESERVER2_CLEAN_TASK_H_
