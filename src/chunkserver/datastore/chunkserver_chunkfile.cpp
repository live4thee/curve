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
 * File Created: Thursday, 6th September 2018 10:49:53 am
 * Author: yangyaokai
 */
#include <fcntl.h>
#include <algorithm>
#include <memory>

#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "src/chunkserver/datastore/chunkserver_chunkfile.h"
#include "src/common/crc32.h"

namespace curve {
namespace chunkserver {

namespace {

bool ValidMinIoAlignment(const char* flagname, uint32_t value) {
    return common::is_aligned(value, 512);
}

}  // namespace

DEFINE_uint32(minIoAlignment, 512,
              "minimum alignment for io request, must align to 512");

DEFINE_validator(minIoAlignment, ValidMinIoAlignment);

ChunkFileMetaPage::ChunkFileMetaPage(const ChunkFileMetaPage& metaPage) {
    version = metaPage.version;
    sn = metaPage.sn;
    correctedSn = metaPage.correctedSn;
    location = metaPage.location;
    if (metaPage.bitmap != nullptr) {
        bitmap = std::make_shared<Bitmap>(metaPage.bitmap->Size(),
                                          metaPage.bitmap->GetBitmap());
    } else {
        bitmap = nullptr;
    }
}

ChunkFileMetaPage& ChunkFileMetaPage::operator =(
    const ChunkFileMetaPage& metaPage) {
    if (this == &metaPage)
        return *this;
    version = metaPage.version;
    sn = metaPage.sn;
    correctedSn = metaPage.correctedSn;
    location = metaPage.location;
    if (metaPage.bitmap != nullptr) {
        bitmap = std::make_shared<Bitmap>(metaPage.bitmap->Size(),
                                          metaPage.bitmap->GetBitmap());
    } else {
        bitmap = nullptr;
    }
    return *this;
}

void ChunkFileMetaPage::encode(char* buf) {
    size_t len = 0;
    memcpy(buf, &version, sizeof(version));
    len += sizeof(version);
    memcpy(buf + len, &sn, sizeof(sn));
    len += sizeof(sn);
    memcpy(buf + len, &correctedSn, sizeof(correctedSn));
    len += sizeof(correctedSn);
    size_t loc_size = location.size();
    memcpy(buf + len, &loc_size, sizeof(loc_size));
    len += sizeof(loc_size);
    // CloneChunk need serialized location information and bitmap information
    if (loc_size > 0) {
        memcpy(buf + len, location.c_str(), loc_size);
        len += loc_size;
        uint32_t bits = bitmap->Size();
        memcpy(buf + len, &bits, sizeof(bits));
        len += sizeof(bits);
        size_t bitmapBytes = (bits + 8 - 1) >> 3;
        memcpy(buf + len, bitmap->GetBitmap(), bitmapBytes);
        len += bitmapBytes;
    }
    uint32_t crc = ::curve::common::CRC32(buf, len);
    memcpy(buf + len, &crc, sizeof(crc));
}

CSErrorCode ChunkFileMetaPage::decode(const char* buf) {
    size_t len = 0;
    memcpy(&version, buf, sizeof(version));
    len += sizeof(version);
    memcpy(&sn, buf + len, sizeof(sn));
    len += sizeof(sn);
    memcpy(&correctedSn, buf + len, sizeof(correctedSn));
    len += sizeof(correctedSn);
    size_t loc_size;
    memcpy(&loc_size, buf + len, sizeof(loc_size));
    len += sizeof(loc_size);
    if (loc_size > 0) {
        location = string(buf + len, loc_size);
        len += loc_size;
        uint32_t bits = 0;
        memcpy(&bits, buf + len, sizeof(bits));
        len += sizeof(bits);
        bitmap = std::make_shared<Bitmap>(bits, buf + len);
        size_t bitmapBytes = (bitmap->Size() + 8 - 1) >> 3;
        len += bitmapBytes;
    }
    uint32_t crc =  ::curve::common::CRC32(buf, len);
    uint32_t recordCrc;
    memcpy(&recordCrc, buf + len, sizeof(recordCrc));
    // check crc
    if (crc != recordCrc) {
        LOG(ERROR) << "Checking Crc32 failed.";
        return CSErrorCode::CrcCheckError;
    }

    // TODO(yyk) check version compatibility, currrent simple error handing,
    // need detailed implementation later
    if (version != FORMAT_VERSION) {
        LOG(ERROR) << "File format version incompatible."
                    << "file version: "
                    << static_cast<uint32_t>(version)
                    << ", format version: "
                    << static_cast<uint32_t>(FORMAT_VERSION);
        return CSErrorCode::IncompatibleError;
    }
    return CSErrorCode::Success;
}

CSChunkFile::CSChunkFile(std::shared_ptr<LocalFileSystem> lfs,
                         std::shared_ptr<FilePool> chunkFilePool,
                         const ChunkOptions& options)
    : size_(options.chunkSize),
      pageSize_(options.pageSize),
      chunkId_(options.id),
      baseDir_(options.baseDir),
      snapshots_(std::make_shared<CSSnapshots>(options.pageSize)),
      chunkFilePool_(chunkFilePool),
      lfs_(lfs),
      metric_(options.metric),
      enableOdsyncWhenOpenChunkFile_(options.enableOdsyncWhenOpenChunkFile) {
    CHECK(!baseDir_.empty()) << "Create chunk file failed";
    CHECK(lfs_ != nullptr) << "Create chunk file failed";
    sn_ = options.sn;
    cloneFileId_ = options.cloneFileId;
    if (metric_ != nullptr) {
        metric_->chunkFileCount << 1;
    }
}

CSChunkFile::~CSChunkFile() {
    if (metric_ != nullptr) {
        metric_->chunkFileCount << -1;
    }
}

CSErrorCode CSChunkFile::Open(bool createFile) {
    WriteLockGuard writeGuard(rwLock_);
    ChunkOptions options;
    options.id = chunkId_;
    options.sn = sn_;
    options.cloneFileId = cloneFileId_;
    options.baseDir = baseDir_;
    options.chunkSize = size_;
    options.pageSize = pageSize_;
    options.metric = metric_;
    options.enableOdsyncWhenOpenChunkFile = enableOdsyncWhenOpenChunkFile_;
    CSSnapshot* snapshot = new (std::nothrow) CSSnapshot(lfs_,
                                          chunkFilePool_,
                                          options);
    CHECK(snapshot != nullptr) << "Failed to new CSSnapshot!";
    CSErrorCode errorCode = snapshot->Open(createFile);
    if (errorCode != CSErrorCode::Success) {
        delete snapshot;
        snapshot = nullptr;
        LOG(ERROR) << "Create first chunk/snapshot failed."
                   << "ChunkID: " << chunkId_
                   << ",request sn: " << sn_;
        return errorCode;
    }

    snapshots_->insert(snapshot);
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::LoadSnapshot(SequenceNum sn) {
    WriteLockGuard writeGuard(rwLock_);
    return loadSnapshot(sn);
}

CSErrorCode CSChunkFile::loadSnapshot(SequenceNum sn) {
    if (snapshots_->contains(sn)) {
        LOG(ERROR) << "Multiple snapshot file found with same SeqNum."
                   << " ChunkID: " << chunkId_
                   << " Snapshot sn: " << sn;
        return CSErrorCode::SnapshotConflictError;
    }
    ChunkOptions options;
    options.id = chunkId_;
    options.sn = sn;
    options.baseDir = baseDir_;
    options.chunkSize = size_;
    options.pageSize = pageSize_;
    options.metric = metric_;
    CSSnapshot *snapshot = new(std::nothrow) CSSnapshot(lfs_,
                                            chunkFilePool_,
                                            options);
    CHECK(snapshot != nullptr) << "Failed to new CSSnapshot!"
                                << "ChunkID:" << chunkId_
                                << ",snapshot sn:" << sn;
    CSErrorCode errorCode = snapshot->Open(false);
    if (errorCode != CSErrorCode::Success) {
        delete snapshot;
        snapshot = nullptr;
        LOG(ERROR) << "Load snapshot failed."
                   << "ChunkID: " << chunkId_
                   << ",snapshot sn: " << sn;
        return errorCode;
    }
    snapshots_->insert(snapshot);

    return errorCode;
}

CSErrorCode CSChunkFile::Write(SequenceNum sn,
                               const butil::IOBuf& buf,
                               off_t offset,
                               size_t length,
                               uint32_t* cost,
                               std::shared_ptr<SnapContext> ctx) {
    WriteLockGuard writeGuard(rwLock_);
    if (!CheckOffsetAndLength(
            offset, length, pageSize_)) {
        LOG(ERROR) << "Write chunk failed, invalid offset or length."
                   << "ChunkID: " << chunkId_
                   << ", offset: " << offset
                   << ", length: " << length
                   << ", page size: " << pageSize_
                   << ", chunk size: " << size_
                   << ", align: " << pageSize_;
        return CSErrorCode::InvalidArgError;
    }

    // When it is the first time to write chunk after rollback, chunk file
    // needs to be created here.
    if (createChunkIfNotExist(sn, ctx) != CSErrorCode::Success) {
        return CSErrorCode::InternalError;
    }
    // Curve will ensure that all previous requests arrive or time out
    // before issuing new requests after user initiate a snapshot request.
    // Therefore, this is only a log recovery request, and it must have been
    // executed, and an error code can be returned here.
    SequenceNum latestSn = snapshots_->getLatestFile()->GetSn();
    if (sn < latestSn) {
        LOG(WARNING) << "Backward write request."
                     << "ChunkID: " << chunkId_
                     << ",request sn: " << sn
                     << ", latest sn: " << latestSn;
        return CSErrorCode::BackwardRequestError;
    }

    // If the requested sequence number is greater than the current chunk
    // sequence number, the metapage needs to be updated
    if (sn > latestSn) {
        // update metapage and adjust the location in map accordingly
        CSErrorCode errorCode = snapshots_->Move(latestSn, sn);
        if (errorCode != CSErrorCode::Success) {
            return errorCode;
        }
        errorCode = loadSnapshot(sn);
        if (errorCode != CSErrorCode::Success) {
            return errorCode;
        }
        latestSn = snapshots_->getLatestFile()->GetSn();
    }

    // Determine whether to create a snapshot file
    if (needCreateSnapshot(sn, ctx)) {
        CSErrorCode err = createSnapshot(ctx->getLatest(sn), ctx->getCloneFileInfo(sn).id());
        if (err != CSErrorCode::Success) {
            return err;
        }
    }

    // If it is cow, copy the data to the snapshot file first
    if (needCow(sn, ctx)) {
        CSErrorCode errorCode = copy2Snapshot(offset, length, sn, ctx);
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Copy data to snapshot failed."
                        << "ChunkID: " << chunkId_
                        << ",request sn: " << sn
                        << ", latest sn: " << latestSn;
            return errorCode;
        }
    }
    CSErrorCode rc = snapshots_->getLatestFile()->Write(buf, offset, length);
    if (rc != CSErrorCode::Success) {
        LOG(ERROR) << "Write data to chunk file failed."
                   << "ChunkID: " << chunkId_
                   << ",request sn: " << sn
                   << ", latest sn: " << latestSn;
        return CSErrorCode::InternalError;
    }
    CSErrorCode errorCode = snapshots_->getLatestFile()->Flush();
    if (errorCode != CSErrorCode::Success) {
        LOG(ERROR) << "Write data to chunk file failed."
                   << "ChunkID: " << chunkId_
                   << ",request sn: " << sn
                   << ", latest sn: " << latestSn;
        return errorCode;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::createSnapshot(SequenceNum sn, SequenceNum cloneFileId) {
    if (snapshots_->contains(sn)) {
        return CSErrorCode::Success;
    }

    // create snapshot
    ChunkOptions options;
    options.id = chunkId_;
    options.sn = sn;
    options.cloneFileId = cloneFileId;
    options.baseDir = baseDir_;
    options.chunkSize = size_;
    options.pageSize = pageSize_;
    options.metric = metric_;
    options.enableOdsyncWhenOpenChunkFile = enableOdsyncWhenOpenChunkFile_;
    auto snapshot = new (std::nothrow) CSSnapshot(lfs_,
                                                   chunkFilePool_,
                                                   options);
    CHECK(snapshot != nullptr) << "Failed to new CSSnapshot!";
    CSErrorCode errorCode = snapshot->Open(true);
    if (errorCode != CSErrorCode::Success) {
        delete snapshot;
        snapshot = nullptr;
        LOG(ERROR) << "Create snapshot failed."
                   << "ChunkID: " << chunkId_
                   << ",request sn: " << sn;
        return errorCode;
    }

    snapshots_->insert(snapshot);
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::Sync() {
    WriteLockGuard writeGuard(rwLock_);
    return snapshots_->Sync();
}

CSErrorCode CSChunkFile::Paste(const char * buf, off_t offset, size_t length) {
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::Read(char * buf, off_t offset, size_t length) {
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::ReadSpecifiedChunk(SequenceNum sn,
                                            char * buf,
                                            off_t offset,
                                            size_t length,
                                            std::shared_ptr<SnapContext> ctx)  {
    ReadLockGuard readGuard(rwLock_);
    if (!CheckOffsetAndLength(offset, length, pageSize_)) {
        LOG(ERROR) << "Read specified chunk failed, invalid offset or length."
                   << "ChunkID: " << chunkId_
                   << ", offset: " << offset
                   << ", length: " << length
                   << ", page size: " << pageSize_
                   << ", chunk size: " << size_
                   << ", align: " << pageSize_;
        return CSErrorCode::InvalidArgError;
    }
    return snapshots_->Read(sn, buf, offset, length, ctx);
}

CSErrorCode CSChunkFile::Delete(SequenceNum sn)  {
    WriteLockGuard writeGuard(rwLock_);
    // If sn is less than the current sequence of the chunk, can not be deleted
    SequenceNum latestSn = snapshots_->getLatestFile()->GetSn();
    if (sn < latestSn) {
        LOG(WARNING) << "Delete chunk failed, backward request."
                     << "ChunkID: " << chunkId_
                     << ", request sn: " << sn
                     << ", latest sn: " << latestSn;
        return CSErrorCode::BackwardRequestError;
    }

    CSErrorCode ret = snapshots_->DeleteAll();
    if (ret != CSErrorCode::Success)
        return ret;

    LOG(INFO) << "Chunk deleted."
              << "ChunkID: " << chunkId_
              << ", latest sn: " << latestSn
              << ", request sn: " << sn;
    return CSErrorCode::Success;
}

/**
 * This method is not only used to delete snapshot created by user,
 * but also used to delete innner snapshot which is created by mds 
 * when file rollbacks. This inner snapshot has sn which equals to 
 * the seqnum of the clonefile.
*/
CSErrorCode CSChunkFile::DeleteSnapshot(SequenceNum snapSn, std::shared_ptr<SnapContext> ctx) {
    WriteLockGuard writeGuard(rwLock_);

    // It means deleting inner snapshot. Theoretically there's at most only working
    // chunk left within the clone file and we only need to delete that chunk.
    if (ctx->getCloneFileInfo(snapSn).seqnum() == snapSn) {
        return snapshots_->DeleteWorkingChunk(snapSn, ctx);
    }

    if (!snapshots_->contains(snapSn)) {
        return CSErrorCode::Success;
    }
    CSSnapshot* curFile = snapshots_->getCurrentFile(snapSn, ctx);
    if (!curFile) {
        LOG(ERROR) << "DeleteSnapshot sn " << snapSn << " find no working chunk, which should not happen!";
        return CSErrorCode::InternalError;
    }
    /*
     * If sn of the snapshot to be deleted is equal or larger than the sn of current working 
     * chunk, it means no COW generated at this snapshot and thus no need to delete.
     * Under this circumstance, there will be snapshot which resides in snapshots_ but not in ctx.
     * For example, current working chunk (chunk with largest sn) in snapshots_ has sn of value 3,
     * and the snaps in ctx is {1,2,3,4}, a snapshot deletion with sn = 3 will delete nothing in
     * snapshot_ and ctx becomes {1,2,4}, resulting snapshot of sn 3 in snapshots_ but not in ctx.
    */
    if (curFile->GetSn() > snapSn) {
        return snapshots_->Delete(this, snapSn, ctx);
    }
    return CSErrorCode::Success;
}

void CSChunkFile::GetInfo(CSChunkInfo* info)  {
    ReadLockGuard readGuard(rwLock_);
    // TODO: this method no use anymore, should be deleted
    return;
}

CSErrorCode CSChunkFile::GetHash(off_t offset,
                                 size_t length,
                                 std::string* hash)  {
    ReadLockGuard readGuard(rwLock_);
    uint32_t crc32c = 0;

    char *buf = new(std::nothrow) char[length];
    if (nullptr == buf) {
        return CSErrorCode::InternalError;
    }

    CSSnapshot* file = snapshots_->getLatestFile();
    if (nullptr == file) {
        return CSErrorCode::ChunkNotExistError;
    }

    int rc = file->Read(buf, offset, length);
    if (rc < 0) {
        LOG(ERROR) << "Read chunk file failed."
                   << "latest sn: " << file->GetSn()
                   << "ChunkID: " << chunkId_;
        delete[] buf;
        return CSErrorCode::InternalError;
    }

    crc32c = curve::common::CRC32(crc32c, buf, length);
    *hash = std::to_string(crc32c);

    delete[] buf;

    return CSErrorCode::Success;
}

bool CSChunkFile::needCreateSnapshot(SequenceNum sn, std::shared_ptr<SnapContext> ctx) {
    return !ctx->empty(sn) && !snapshots_->contains(ctx->getLatest(sn));
}

bool CSChunkFile::needCow(SequenceNum sn, std::shared_ptr<SnapContext> ctx) {
    // There is no snapshots thus no need to do cow
    if (ctx->empty(sn))
        return false;

    SequenceNum latestSn = snapshots_->getLatestFile()->GetSn();
    SequenceNum chunkSn = std::max(ctx->getLatest(sn), latestSn);
    // Requests smaller than chunkSn will be rejected directly
    if (sn < chunkSn)
        return false;

    // The preceding logic ensures that the sn here must be equal to metaPage.sn
    // Because if sn<metaPage_.sn, the request will be rejected
    // When sn>metaPage_.sn, metaPage.sn will be updated to sn first
    // And because snapSn is normally smaller than metaPage_.sn, snapSn should
    // also be smaller than sn
    // There may be several situations where metaPage_.sn <= snap.sn
    // Scenario 1: DataStore restarts to restore historical logs,
    // metaPage_.sn==snap.sn may appear
    // There was a request to generate a snapshot file before the restart,
    // but it restarted before the metapage was updated
    // After restarting, the previous operation is played back, and the sn of
    // this operation is equal to the sn of the current chunk
    // Scenario 2: The follower downloads a snapshot of the raft through the
    // leader when restoring the raft
    // During the download process, the chunk on the leader is also taking a
    // snapshot of the chunk, and the follower will do log recovery after
    // downloading
    // Since follower downloads the chunk file first, and then downloads the
    // snapshot file, so at this time metaPage_.sn<=snap.sn
    if (sn != latestSn || latestSn <= snapshots_->getCurrentSnapSn(sn, ctx)) {
        LOG(WARNING) << "May be a log replay opt after an unexpected restart."
                     << "Request sn: " << sn
                     << ", latest sn: " << latestSn
                     << ", snapshot sn: " << snapshots_->getCurrentSnapSn(sn, ctx);
        return false;
    }
    return true;
}

CSErrorCode CSChunkFile::copy2Snapshot(off_t offset, size_t length, SequenceNum sn, std::shared_ptr<SnapContext> ctx) {
    // Get the uncopied area in the snapshot file
    uint32_t pageBeginIndex = offset / pageSize_;
    uint32_t pageEndIndex = (offset + length - 1) / pageSize_;
    std::vector<BitRange> uncopiedRange;
    CSSnapshot* snapshot = snapshots_->getCurrentSnapshot(sn, ctx);
    std::shared_ptr<const Bitmap> snapBitmap = snapshot->GetPageStatus();
    snapBitmap->Divide(pageBeginIndex,
                       pageEndIndex,
                       &uncopiedRange,
                       nullptr);

    CSErrorCode errorCode = CSErrorCode::Success;
    off_t copyOff;
    size_t copySize;
    // Read the uncopied area from the working chunk
    // and write it to the snapshot file
    for (auto& range : uncopiedRange) {
        copyOff = range.beginIndex * pageSize_;
        copySize = (range.endIndex - range.beginIndex + 1) * pageSize_;
        std::shared_ptr<char> buf(new char[copySize],
                                  std::default_delete<char[]>());

        int rc = snapshots_->Read(sn, buf.get(), copyOff, copySize, ctx);
        if (rc < 0) {
            LOG(ERROR) << "Read from chunk file failed."
                       << "ChunkID: " << chunkId_;
            return CSErrorCode::InternalError;
        }
        errorCode = snapshot->Write(buf.get(), copyOff, copySize);
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Write to snapshot failed."
                       << "ChunkID: " << chunkId_
                       << ",snapshot sn: " << snapshot->GetSn();
            return errorCode;
        }
    }
    // If the snapshot file has been written,
    // you need to call Flush to persist the metapage
    if (uncopiedRange.size() > 0) {
        errorCode = snapshot->Flush();
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Flush snapshot metapage failed."
                        << "ChunkID: " << chunkId_
                        << ",snapshot sn: " << snapshot->GetSn();
            return errorCode;
        }
    }
    return CSErrorCode::Success;
}

CSErrorCode CSChunkFile::createChunkIfNotExist(SequenceNum sn, std::shared_ptr<SnapContext> ctx) {
    if (nullptr == snapshots_->getCurrentFile(sn, ctx)) {
        // this newly created snapshot is the "working chunk"
        CSErrorCode err = createSnapshot(sn, ctx->getCloneFileInfo(sn).id());
        if (err != CSErrorCode::Success) {
            return err;
        }
    }
    return CSErrorCode::Success;
}

}  // namespace chunkserver
}  // namespace curve
