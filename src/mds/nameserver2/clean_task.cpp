/*
 *  Copyright (c) 2023 Shanghai Yunzhou Information and Technology Ltd.
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
 * File Created: Wed. 19th July 2023
 * Author: zhu zhengwei
 */


#include "src/mds/nameserver2/clean_task.h"

namespace curve {
namespace mds {

void SnapShotBatchCleanTask::Run(void) {
    do {
        auto task = front();
        if (!task) {
            LOG(INFO) << "SnapShotBatchCleanTask run finished, taskid = " << GetTaskID();
            GetMutableTaskProgress()->SetProgress(100);
            GetMutableTaskProgress()->SetStatus(TaskStatus::SUCCESS);
            return;
        }

        LOG(INFO) << "Ready to clean snapshot " << task->GetMutableFileInfo()->filename()
                    << ", batch taskid = " << GetTaskID();
        // CAUTION: Fill the snaps field of snapshot fileinfo with currently
        //          existing snapshot file seqnum prior to the snap to be deleted
        if (!setCurrentExistingSnaps(task->GetMutableFileInfo())) {
            LOG(ERROR) << "Unable to get snaps from storage, clean task failed."
                    << " batch taskid = " << GetTaskID();
            GetMutableTaskProgress()->SetStatus(TaskStatus::FAILED);
            return;
        }
        // Start to do snapshot clean task synchronously.
        task->Run();
        if (task->GetTaskProgress().GetStatus() == TaskStatus::SUCCESS) {
            LOG(INFO) << "Snapshot " << task->GetMutableFileInfo()->filename()
                    << " cleaned success, batch taskid = " << GetTaskID();
            pop(task->GetMutableFileInfo()->seqnum());
        } else {
            // Notify CleanTaskManager with failed status and may try 
            // again before exceeding retry times 
            LOG(INFO) << "Snapshot " << task->GetMutableFileInfo()->filename()
                    << " cleaned failed, batch taskid = " << GetTaskID();
            GetMutableTaskProgress()->SetStatus(TaskStatus::FAILED);
            return;
        }
    } while (true);
}

bool SnapShotBatchCleanTask::PushTask(const FileInfo &snapfileInfo) {
    common::LockGuard lck(mutexSnapTask_);
    
    if (cleanOrderedSnapTasks_.find(snapfileInfo.seqnum()) != cleanOrderedSnapTasks_.end()) {
        return false;
    }
    auto task = std::make_shared<SnapShotCleanTask2>(static_cast<TaskIDType>(snapfileInfo.seqnum()), 
                                cleanCore_, snapfileInfo, asyncEntity_, mdsSessionTimeUs_);
    task->StartTimer();
    cleanOrderedSnapTasks_.insert(std::make_pair(static_cast<SeqNum>(snapfileInfo.seqnum()), task));
    LOG(INFO) << "SnapShotBatchCleanTask push snapshot " << snapfileInfo.filename()
                << ", to be deleted snapshot count = " << cleanOrderedSnapTasks_.size()
                << ", batch taskid = " << GetTaskID();
    return true;
}

std::shared_ptr<Task> SnapShotBatchCleanTask::GetTask(SeqNum sn) {
    common::LockGuard lck(mutexSnapTask_);

    auto iter = cleanOrderedSnapTasks_.find(sn);
    if (iter == cleanOrderedSnapTasks_.end()) {
        return nullptr;
    } else {
        return iter->second;
    }        
}

bool SnapShotBatchCleanTask::IsEmpty() {
    common::LockGuard lck(mutexSnapTask_);
    return cleanOrderedSnapTasks_.empty();
} 

std::shared_ptr<SnapShotCleanTask2> SnapShotBatchCleanTask::front() {
    common::LockGuard lck(mutexSnapTask_);
    auto iter = cleanOrderedSnapTasks_.begin();
    if (iter == cleanOrderedSnapTasks_.end()) {
        return nullptr;
    }
    return iter->second;
}

void SnapShotBatchCleanTask::pop(SeqNum sn) {
    common::LockGuard lck(mutexSnapTask_);
    cleanOrderedSnapTasks_.erase(sn);
    LOG(INFO) << "SnapShotBatchCleanTask pop snapshot " << sn
                << ", remain snapshot count = " << cleanOrderedSnapTasks_.size()
                << ", batch taskid = " << GetTaskID();
}

bool SnapShotBatchCleanTask::setCurrentExistingSnaps(FileInfo* snapshotInfo) {
    // list snapshot files
    std::vector<FileInfo> snapShotFileInfos;
    StoreStatus storeStatus =  storage_->ListSnapshotFile(snapshotInfo->parentid(),
                                        snapshotInfo->parentid() + 1,
                                        &snapShotFileInfos);
    if (storeStatus != StoreStatus::KeyNotExist &&
        storeStatus != StoreStatus::OK) {
        LOG(ERROR) << "snapshot name " << snapshotInfo->filename() 
                    << ", storage ListSnapshotFile return " << storeStatus;
        return false;
    } 
    // There is no need to set the entire clonefileinfos, so we fake a clone file.
    auto cloneFileId = snapshotInfo->clones(snapshotInfo->clones_size() - 1).id();
    snapshotInfo->clear_clones();
    auto fakeClone = snapshotInfo->add_clones();
    // fake seqnum of the clone, there are two scenario:
    // 1. it's an user-created snapshot so the seqnum needs to be larger than snapshot sn to be logical
    // 2. it's an inner snapshot representing the old rollback file which is to be deleted
    // chunkserver delete these two kind of chunk in a different way
    const std::string inner = "inner";
    const std::string snapName = snapshotInfo->filename();
    // inner snapshot has filename ended with 'inner'
    if (snapName.length() > inner.length() && 
        snapName.rfind(inner) == snapName.length() - inner.length()) {
        fakeClone->set_seqnum(snapshotInfo->seqnum());
    } else {
        fakeClone->set_seqnum(snapshotInfo->seqnum() + 1);
    }
    fakeClone->set_id(cloneFileId);
    // Must order by seqnum of the snapshot beforehand!
    std::sort(snapShotFileInfos.begin(), snapShotFileInfos.end(), 
             [] (FileInfo& a, FileInfo& b) {return a.seqnum() < b.seqnum();});

    for (FileInfo& existingSnap : snapShotFileInfos) {
        // 1. Judge if the snapshot is within the same clone file of the snapshot
        //    to be deleted by comparing id.
        // 2. only snaps prior to the deleted snapshot matter.
        if (existingSnap.seqnum() < snapshotInfo->seqnum() && 
            existingSnap.clones(existingSnap.clones_size() - 1).id() 
            == fakeClone->id()) {
            fakeClone->add_snaps(existingSnap.seqnum()); 
        }
    }
    return true;
}

}  // namespace mds
}  // namespace curve
