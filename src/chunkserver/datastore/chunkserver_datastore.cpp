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
 * File Created: Wednesday, 5th September 2018 8:04:03 pm
 * Author: yangyaokai
 */

#include <gflags/gflags.h>
#include <fcntl.h>
#include <cstring>
#include <iostream>
#include <list>
#include <memory>

#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "src/chunkserver/datastore/filename_operator.h"
#include "src/common/location_operator.h"

namespace curve {
namespace chunkserver {

CSDataStore::CSDataStore(std::shared_ptr<LocalFileSystem> lfs,
                         std::shared_ptr<FilePool> chunkFilePool,
                         const DataStoreOptions& options)
    : chunkSize_(options.chunkSize),
      pageSize_(options.pageSize),
      baseDir_(options.baseDir),
      locationLimit_(options.locationLimit),
      chunkFilePool_(chunkFilePool),
      lfs_(lfs),
      enableOdsyncWhenOpenChunkFile_(options.enableOdsyncWhenOpenChunkFile) {
    CHECK(!baseDir_.empty()) << "Create datastore failed";
    CHECK(lfs_ != nullptr) << "Create datastore failed";
    CHECK(chunkFilePool_ != nullptr) << "Create datastore failed";
}

CSDataStore::~CSDataStore() {
}

bool CSDataStore::Initialize() {
    // Make sure the baseDir directory exists
    if (!lfs_->DirExists(baseDir_.c_str())) {
        int rc = lfs_->Mkdir(baseDir_.c_str());
        if (rc < 0) {
            LOG(ERROR) << "Create " << baseDir_ << " failed.";
            return false;
        }
    }

    vector<string> files;
    int rc = lfs_->List(baseDir_, &files);
    if (rc < 0) {
        LOG(ERROR) << "List " << baseDir_ << " failed.";
        return false;
    }

    // If loaded before, reload here
    metaCache_.Clear();
    metric_ = std::make_shared<DataStoreMetric>();
    for (size_t i = 0; i < files.size(); ++i) {
        FileNameOperator::FileInfo info =
            FileNameOperator::ParseFileName(files[i]);
        if (info.type == FileNameOperator::FileType::CHUNK) {
            LOG(WARNING) << "Find chunk file " << files[i] << ", which should not happen";
        } else if (info.type == FileNameOperator::FileType::SNAPSHOT) {
            // For instant rollback, on disk chunk file no longer exists.
            // Only chunk snapshot files exist, so load file in loadSnapshot method afterwards            
            if (metaCache_.Get(info.id) == nullptr) {
                ChunkOptions options;
                options.id = info.id;
                options.sn = 0;
                options.baseDir = baseDir_;
                options.chunkSize = chunkSize_;
                options.pageSize = pageSize_;
                options.metric = metric_;
                CSChunkFilePtr chunkFilePtr =
                    std::make_shared<CSChunkFile>(lfs_,
                                                chunkFilePool_,
                                                options);

                metaCache_.Set(info.id, chunkFilePtr);
            }

            // Load snapshot to memory
            CSErrorCode errorCode = metaCache_.Get(info.id)->LoadSnapshot(info.sn);
            if (errorCode != CSErrorCode::Success) {
                LOG(ERROR) << "Load snapshot failed.";
                return false;
            }
        } else {
            LOG(WARNING) << "Unknown file: " << files[i];
        }
    }
    LOG(INFO) << "Initialize data store success with " << files.size() 
              << " files in dir " << baseDir_;
    return true;
}

/**
 * It's guaranteed by control plane (MDS) that the chunk can be deleted 
 * when meeting the following conditions: 
 * a. all of the snapshots have been cleared
 * b. or all of the snapshots are to be cleared
*/
CSErrorCode CSDataStore::DeleteChunk(ChunkID id, SequenceNum sn) {
    auto chunkFile = metaCache_.Get(id);
    if (chunkFile != nullptr) {
        CSErrorCode errorCode = chunkFile->Delete(sn);
        if (errorCode != CSErrorCode::Success) {
            LOG(WARNING) << "Delete chunk file failed."
                         << "ChunkID = " << id;
            return errorCode;
        }
        metaCache_.Remove(id);
    }
    return CSErrorCode::Success;
}

CSErrorCode CSDataStore::DeleteSnapshotChunk(
    ChunkID id, SequenceNum snapSn, std::shared_ptr<SnapContext> ctx) {
    auto chunkFile = metaCache_.Get(id);
    if (chunkFile != nullptr) {
        CSErrorCode errorCode = chunkFile->DeleteSnapshot(snapSn, ctx);  // NOLINT
        if (errorCode != CSErrorCode::Success) {
            LOG(WARNING) << "Delete snapshot chunk failed."
                         << "ChunkID = " << id
                         << ", snapSn = " << snapSn;
            return errorCode;
        }
    }
    return CSErrorCode::Success;
}

CSErrorCode CSDataStore::ReadChunk(ChunkID id,
                                   SequenceNum sn,
                                   char * buf,
                                   off_t offset,
                                   size_t length,
                                   std::shared_ptr<SnapContext> ctx) {
    if (!ctx) {
        return CSErrorCode::InvalidArgError;
    }
    if (sn != ctx->getCurrentFileSn()) {
        LOG(ERROR) << "Sequence num " << sn 
                   << " is not equal to current file sn " << ctx->getCurrentFileSn()
                   << " ChunkID = " << id;
        return CSErrorCode::InvalidArgError;
    }
    // we treat chunk and chunk snapshot as the same
    return ReadSnapshotChunk(id, sn, buf, offset, length, ctx);
}

// It is ensured that if snap chunk exists, the chunk must exist.
// 1. snap chunk is generated from COW, thus chunk must exist.
// 2. discard will not delete chunk if there is snapshot.
CSErrorCode CSDataStore::ReadSnapshotChunk(ChunkID id,
                                           SequenceNum sn,
                                           char * buf,
                                           off_t offset,
                                           size_t length,
                                           std::shared_ptr<SnapContext> ctx) {
    if (!ctx) {
        return CSErrorCode::InvalidArgError;
    }
    auto chunkFile = metaCache_.Get(id);
    if (chunkFile == nullptr) {
        return CSErrorCode::ChunkNotExistError;
    }
    if (!ctx->contains(sn)) {
        LOG(ERROR) << "Read snapshot chunk SnapshotNotExistError, sn = " << sn
                   << ", ChunkID = " << id;
        return CSErrorCode::SnapshotNotExistError;
    }
    CSErrorCode errorCode =
        chunkFile->ReadSpecifiedChunk(sn, buf, offset, length, ctx);
    if (errorCode != CSErrorCode::Success) {
        LOG(WARNING) << "Read snapshot chunk failed."
                     << "ChunkID = " << id;
    }
    return errorCode;
}

CSErrorCode CSDataStore::CreateChunkFile(const ChunkOptions & options,
                                         CSChunkFilePtr* chunkFile) {
        if (!options.location.empty() &&
            options.location.size() > locationLimit_) {
            LOG(ERROR) << "Location is too long."
                       << "ChunkID = " << options.id
                       << ", location = " << options.location
                       << ", location size = " << options.location.size()
                       << ", location limit size = " << locationLimit_;
            return CSErrorCode::InvalidArgError;
        }
        auto tempChunkFile = std::make_shared<CSChunkFile>(lfs_,
                                                  chunkFilePool_,
                                                  options);
        CSErrorCode errorCode = tempChunkFile->Open(true);
        if (errorCode != CSErrorCode::Success) {
            LOG(WARNING) << "Create chunk file failed."
                         << "ChunkID = " << options.id
                         << ", ErrorCode = " << errorCode;
            return errorCode;
        }
        // If there are two operations concurrently to create a chunk file,
        // Then the chunkFile generated by one of the operations will be added
        // to metaCache first, the subsequent operation abandons the currently
        // generated chunkFile and uses the previously generated chunkFile
        *chunkFile = metaCache_.Set(options.id, tempChunkFile);
        return CSErrorCode::Success;
}


CSErrorCode CSDataStore::WriteChunk(ChunkID id,
                            SequenceNum sn,
                            const butil::IOBuf& buf,
                            off_t offset,
                            size_t length,
                            uint32_t* cost,
                            std::shared_ptr<SnapContext> ctx,
                            const std::string & cloneSourceLocation)  {
    // The requested sequence number is not allowed to be 0, when snapsn=0,
    // it will be used as the basis for judging that the snapshot does not exist
    if (sn == kInvalidSeq) {
        LOG(ERROR) << "Sequence num should not be zero."
                   << "ChunkID = " << id;
        return CSErrorCode::InvalidArgError;
    }

    if (sn != ctx->getCurrentFileSn()) {
        LOG(ERROR) << "Sequence num " << sn 
                   << " is not equal to current file sn " << ctx->getCurrentFileSn()
                   << " ChunkID = " << id;
        return CSErrorCode::InvalidArgError;
    }
    auto chunkFile = metaCache_.Get(id);
    // If the chunk file does not exist, create the chunk file first
    if (chunkFile == nullptr) {
        ChunkOptions options;
        options.id = id;
        options.sn = sn;
        options.cloneFileId = ctx->getCloneFileInfo(sn).id();
        options.baseDir = baseDir_;
        options.chunkSize = chunkSize_;
        options.location = cloneSourceLocation;
        options.pageSize = pageSize_;
        options.metric = metric_;
        options.enableOdsyncWhenOpenChunkFile = enableOdsyncWhenOpenChunkFile_;
        CSErrorCode errorCode = CreateChunkFile(options, &chunkFile);
        if (errorCode != CSErrorCode::Success) {
            return errorCode;
        }
    }
    // write chunk file
    CSErrorCode errorCode = chunkFile->Write(sn,
                                             buf,
                                             offset,
                                             length,
                                             cost,
                                             ctx);
    if (errorCode != CSErrorCode::Success) {
        LOG(WARNING) << "Write chunk file failed."
                     << "ChunkID = " << id;
        return errorCode;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSDataStore::SyncChunk(ChunkID id) {
    auto chunkFile = metaCache_.Get(id);
    if (chunkFile == nullptr) {
        LOG(WARNING) << "Sync chunk not exist, ChunkID = " << id;
        return CSErrorCode::Success;
    }
    CSErrorCode errorCode = chunkFile->Sync();
    if (errorCode != CSErrorCode::Success) {
        LOG(WARNING) << "Sync chunk file failed."
                     << "ChunkID = " << id;
        return errorCode;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSDataStore::CreateCloneChunk(ChunkID id,
                                          SequenceNum sn,
                                          SequenceNum correctedSn,
                                          ChunkSizeType size,
                                          const string& location) {
    // Check the validity of the parameters
    if (size != chunkSize_
        || sn == kInvalidSeq
        || location.empty()) {
        LOG(ERROR) << "Invalid arguments."
                   << "ChunkID = " << id
                   << ", sn = " << sn
                   << ", size = " << size
                   << ", location = " << location;
        return CSErrorCode::InvalidArgError;
    }
    auto chunkFile = metaCache_.Get(id);
    // If the chunk file does not exist, create the chunk file first
    if (chunkFile == nullptr) {
        ChunkOptions options;
        options.id = id;
        options.sn = sn;
        options.location = location;
        options.baseDir = baseDir_;
        options.chunkSize = chunkSize_;
        options.pageSize = pageSize_;
        options.metric = metric_;
        CSErrorCode errorCode = CreateChunkFile(options, &chunkFile);
        if (errorCode != CSErrorCode::Success) {
            return errorCode;
        }
    }
    // Determine whether the specified parameters match the information
    // in the existing Chunk
    // No need to put in else, because users may call this interface at the
    // same time
    // If different sequence or location information are specified in the
    // parameters, there may be concurrent conflicts, and judgments are also
    // required

    // We may implement clone in a different way later.
    // CSChunkInfo info;
    // chunkFile->GetInfo(&info);
    // if (info.location.compare(location) != 0
    //     || info.curSn != sn) {
    //     LOG(WARNING) << "Conflict chunk already exists."
    //                << "sn in arg = " << sn
    //                << ", location in arg = " << location
    //                << ", sn in chunk = " << info.curSn
    //                << ", location in chunk = " << info.location;
    //     return CSErrorCode::ChunkConflictError;
    // }
    return CSErrorCode::Success;
}

CSErrorCode CSDataStore::PasteChunk(ChunkID id,
                                    const char * buf,
                                    off_t offset,
                                    size_t length) {
    auto chunkFile = metaCache_.Get(id);
    // Paste Chunk requires Chunk must exist
    if (chunkFile == nullptr) {
        LOG(WARNING) << "Paste Chunk failed, Chunk not exists."
                     << "ChunkID = " << id;
        return CSErrorCode::ChunkNotExistError;
    }
    CSErrorCode errcode = chunkFile->Paste(buf, offset, length);
    if (errcode != CSErrorCode::Success) {
        LOG(WARNING) << "Paste Chunk failed, Chunk not exists."
                     << "ChunkID = " << id;
        return errcode;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSDataStore::GetChunkInfo(ChunkID id,
                                      CSChunkInfo* chunkInfo) {
    auto chunkFile = metaCache_.Get(id);
    if (chunkFile == nullptr) {
        LOG(INFO) << "Get ChunkInfo failed, Chunk not exists."
                  << "ChunkID = " << id;
        return CSErrorCode::ChunkNotExistError;
    }
    chunkFile->GetInfo(chunkInfo);
    return CSErrorCode::Success;
}

CSErrorCode CSDataStore::GetChunkHash(ChunkID id,
                                      off_t offset,
                                      size_t length,
                                      std::string* hash) {
    auto chunkFile = metaCache_.Get(id);
    if (chunkFile == nullptr) {
        LOG(INFO) << "Get ChunkHash failed, Chunk not exists."
                  << "ChunkID = " << id;
        return CSErrorCode::ChunkNotExistError;
    }
    return chunkFile->GetHash(offset, length, hash);
}

DataStoreStatus CSDataStore::GetStatus() {
    DataStoreStatus status;
    status.chunkFileCount = metric_->chunkFileCount.get_value();
    status.cloneChunkCount = metric_->cloneChunkCount.get_value();
    status.snapshotCount = metric_->snapshotCount.get_value();
    return status;
}

SnapContext::SnapContext(const CloneFileInfos& cloneFileInfos)
    : cloneFileInfos_(cloneFileInfos) {
}

SequenceNum SnapContext::getPrev(SequenceNum sn) const {
    CloneFileInfo clone = getCloneFileInfo(sn);
    if (clone.seqnum() == sn) {
        return clone.snaps().empty() ? 0 : *clone.snaps().rbegin();
    }
    auto it = std::lower_bound(clone.snaps().begin(), clone.snaps().end(), sn);
    if (it == clone.snaps().begin()) {
        return 0;
    }
    return *--it;
}

SequenceNum SnapContext::getLatest(SequenceNum sn) const {
    CloneFileInfo clone = getCloneFileInfo(sn);
    return clone.snaps().empty() ? 0: *clone.snaps().rbegin();
}

bool SnapContext::contains(SequenceNum sn) const {
    CloneFileInfo clone = getCloneFileInfo(sn);
    if (clone.seqnum() == 0) 
        return false;
    return clone.seqnum() == sn || std::binary_search(clone.snaps().begin(), clone.snaps().end(), sn);
}

bool SnapContext::empty(SequenceNum sn) const {
    CloneFileInfo clone = getCloneFileInfo(sn);
    return clone.snaps().empty();
}

CloneFileInfo SnapContext::getCloneFileInfo(SequenceNum sn) const {
    CloneFileInfo info;
    // the clone file is guaranteed sorted by seqnum() field in ascending order
    // the sn to be found MAY be missing in the clone.snaps() array (in snapshot delete situation)
    for (auto& clone : cloneFileInfos_.clones()) {
        if (sn <= clone.seqnum()) {
            return clone;
        }
    }
    return info;
}

CloneFileInfos SnapContext::getCloneFileInfos() const {
    return cloneFileInfos_;
}

SequenceNum SnapContext::getCurrentFileSn() const {
    return cloneFileInfos_.seqnum();
}

}  // namespace chunkserver
}  // namespace curve
