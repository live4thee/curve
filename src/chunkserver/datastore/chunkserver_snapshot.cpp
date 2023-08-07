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
 * Created Date: Wednesday November 28th 2018
 * Author: yangyaokai
 */

#include <memory>
#include "src/common/bitmap.h"
#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "src/chunkserver/datastore/chunkserver_snapshot.h"
#include "src/fs/wrap_posix.h"

namespace curve {
namespace chunkserver {

void SnapshotMetaPage::encode(char* buf) {
    size_t len = 0;
    memcpy(buf, &version, sizeof(version));
    len += sizeof(version);
    memcpy(buf + len, &damaged, sizeof(damaged));
    len += sizeof(damaged);
    memcpy(buf + len, &sn, sizeof(sn));
    len += sizeof(sn);
    memcpy(buf + len, &cloneFileId, sizeof(cloneFileId));
    len += sizeof(cloneFileId);
    uint32_t bits = bitmap->Size();
    memcpy(buf + len, &bits, sizeof(bits));
    len += sizeof(bits);
    size_t bitmapBytes = (bits + 8 - 1) / 8;
    memcpy(buf + len, bitmap->GetBitmap(), bitmapBytes);
    len += bitmapBytes;
    uint32_t crc = ::curve::common::CRC32(buf, len);
    memcpy(buf + len, &crc, sizeof(crc));
}

CSErrorCode SnapshotMetaPage::decode(const char* buf) {
    size_t len = 0;
    memcpy(&version, buf, sizeof(version));
    len += sizeof(version);
    memcpy(&damaged, buf + len, sizeof(damaged));
    len += sizeof(damaged);
    memcpy(&sn, buf + len, sizeof(sn));
    len += sizeof(sn);
    memcpy(&cloneFileId, buf + len, sizeof(cloneFileId));
    len += sizeof(cloneFileId);
    uint32_t bits = 0;
    memcpy(&bits, buf + len, sizeof(bits));
    len += sizeof(bits);
    bitmap = std::make_shared<Bitmap>(bits, buf + len);
    size_t bitmapBytes = (bitmap->Size() + 8 - 1) / 8;
    len += bitmapBytes;
    uint32_t crc =  ::curve::common::CRC32(buf, len);
    uint32_t recordCrc;
    memcpy(&recordCrc, buf + len, sizeof(recordCrc));
    // Verify crc, return an error code if the verification fails
    if (crc != recordCrc) {
        LOG(ERROR) << "Checking Crc32 failed.";
        return CSErrorCode::CrcCheckError;
    }

    // TODO(yyk) judge version compatibility, simple processing at present,
    // detailed implementation later
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

SnapshotMetaPage::SnapshotMetaPage(const SnapshotMetaPage& metaPage) {
    version = metaPage.version;
    damaged = metaPage.damaged;
    sn = metaPage.sn;
    cloneFileId = metaPage.cloneFileId;
    std::shared_ptr<Bitmap> newMap =
        std::make_shared<Bitmap>(metaPage.bitmap->Size(),
                                 metaPage.bitmap->GetBitmap());
    bitmap = newMap;
}

SnapshotMetaPage& SnapshotMetaPage::operator =(
    const SnapshotMetaPage& metaPage) {
    if (this == &metaPage)
        return *this;
    version = metaPage.version;
    damaged = metaPage.damaged;
    sn = metaPage.sn;
    cloneFileId = metaPage.cloneFileId;
    std::shared_ptr<Bitmap> newMap =
        std::make_shared<Bitmap>(metaPage.bitmap->Size(),
                                 metaPage.bitmap->GetBitmap());
    bitmap = newMap;
    return *this;
}

CSSnapshot::CSSnapshot(std::shared_ptr<LocalFileSystem> lfs,
                       std::shared_ptr<FilePool> chunkFilePool,
                       const ChunkOptions& options)
    : fd_(-1),
      chunkId_(options.id),
      size_(options.chunkSize),
      pageSize_(options.pageSize),
      baseDir_(options.baseDir),
      lfs_(lfs),
      chunkFilePool_(chunkFilePool),
      metric_(options.metric),
      enableOdsyncWhenOpenChunkFile_(options.enableOdsyncWhenOpenChunkFile) {
    CHECK(!baseDir_.empty()) << "Create snapshot failed";
    CHECK(lfs_ != nullptr) << "Create snapshot failed";
    uint32_t bits = size_ / pageSize_;
    metaPage_.bitmap = std::make_shared<Bitmap>(bits);
    metaPage_.sn = options.sn;
    metaPage_.cloneFileId = options.cloneFileId;
    if (metric_ != nullptr) {
        metric_->snapshotCount << 1;
    }
}

CSSnapshot::~CSSnapshot() {
    if (fd_ >= 0) {
        lfs_->Close(fd_);
    }

    if (metric_ != nullptr) {
        metric_->snapshotCount << -1;
    }
}

CSErrorCode CSSnapshot::Open(bool createFile) {
    string snapshotPath = path();
    // Create a new file, if the snapshot file already exists,
    // no need to create it
    // The existence of snapshot files may be caused by the following conditions
    // getchunk succeeded, but failed later in stat or loadmetapage,
    // when the download is opened again;
    if (createFile
        && !lfs_->FileExists(snapshotPath)
        && metaPage_.sn > 0) {
        char buf[pageSize_];  // NOLINT
        memset(buf, 0, sizeof(buf));
        metaPage_.encode(buf);
        int ret = chunkFilePool_->GetFile(snapshotPath, buf);
        if (ret != 0) {
            LOG(ERROR) << "Error occured when create snapshot."
                   << " filepath = " << snapshotPath;
            return CSErrorCode::InternalError;
        }
    }
    int rc = -1;
    if (enableOdsyncWhenOpenChunkFile_) {
        rc = lfs_->Open(snapshotPath, O_RDWR|O_NOATIME|O_DSYNC);
    } else {
        rc = lfs_->Open(snapshotPath, O_RDWR|O_NOATIME);
    }
    if (rc < 0) {
        LOG(ERROR) << "Error occured when opening file."
                   << " filepath = "<< snapshotPath;
        return CSErrorCode::InternalError;
    }
    fd_ = rc;
    struct stat fileInfo;
    rc = lfs_->Fstat(fd_, &fileInfo);
    if (rc < 0) {
        LOG(ERROR) << "Error occured when stating file."
                   << " filepath = " << snapshotPath;
        return CSErrorCode::InternalError;
    }
    if (fileInfo.st_size != fileSize()) {
        LOG(ERROR) << "Wrong file size."
                   << " filepath = " << snapshotPath
                   << ",filesize = " << fileInfo.st_size;
        return CSErrorCode::FileFormatError;
    }

    return loadMetaPage();
}

CSErrorCode CSSnapshot::Read(char * buf, off_t offset, size_t length) {
    // TODO(yyk) Do you need to compare the bit state of the offset?
    int rc = readData(buf, offset, length);
    if (rc < 0) {
        LOG(ERROR) << "Error occured when reading snapshot."
                   << " filepath = "<< path();
        return CSErrorCode::InternalError;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSSnapshot::ReadRanges(char * buf, off_t offset, size_t length, std::vector<BitRange>& ranges) {
    off_t readOff;
    size_t readSize;

    for (auto& range : ranges) {
        readOff = range.beginIndex * pageSize_;
        readSize = (range.endIndex - range.beginIndex + 1) * pageSize_;
        int rc = readData(buf + (readOff - offset),
                          readOff,
                          readSize);
        if (rc < 0) {
            LOG(ERROR) << "Read snap chunk file failed. "
                       << "ChunkID: " << chunkId_
                       << ", snap sn: " << metaPage_.sn;
            return CSErrorCode::InternalError;
        }
    }

    return CSErrorCode::Success;
}

CSErrorCode CSSnapshot::Delete() {
    if (fd_ >= 0) {
        lfs_->Close(fd_);
        fd_ = -1;
    }
    int ret = chunkFilePool_->RecycleFile(path());
    if (ret < 0)
        return CSErrorCode::InternalError;
    return CSErrorCode::Success;
}

SequenceNum CSSnapshot::GetSn() const {
    return metaPage_.sn;
}

std::shared_ptr<const Bitmap> CSSnapshot::GetPageStatus() const {
    return metaPage_.bitmap;
}

SequenceNum CSSnapshot::GetCloneFileId() const {
    return metaPage_.cloneFileId;
}

CSErrorCode CSSnapshot::Write(const char * buf, off_t offset, size_t length) {
    int rc = writeData(buf, offset, length);
    if (rc < 0) {
        LOG(ERROR) << "Write snapshot failed."
                   << "ChunkID: " << chunkId_
                   << ",snapshot sn: " << metaPage_.sn;
        return CSErrorCode::InternalError;
    }
    uint32_t pageBeginIndex = offset / pageSize_;
    uint32_t pageEndIndex = (offset + length - 1) / pageSize_;
    for (uint32_t i = pageBeginIndex; i <= pageEndIndex; ++i) {
        dirtyPages_.insert(i);
    }
    return CSErrorCode::Success;
}

CSErrorCode CSSnapshot::Write(const butil::IOBuf& buf, off_t offset, size_t length) {
    int rc = writeData(buf, offset, length);
    if (rc < 0) {
        LOG(ERROR) << "Write snapshot failed."
                   << "ChunkID: " << chunkId_
                   << ",snapshot sn: " << metaPage_.sn;
        return CSErrorCode::InternalError;
    }
    uint32_t pageBeginIndex = offset / pageSize_;
    uint32_t pageEndIndex = (offset + length - 1) / pageSize_;
    for (uint32_t i = pageBeginIndex; i <= pageEndIndex; ++i) {
        dirtyPages_.insert(i);
    }
    return CSErrorCode::Success;
}

CSErrorCode CSSnapshot::Sync() {
    int rc = SyncData();
    if (rc < 0) {
        LOG(ERROR) << "Sync data failed, "
                   << "ChunkID:" << chunkId_ << ",sn:" << metaPage_.sn;
        return CSErrorCode::InternalError;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSSnapshot::Flush() {
    SnapshotMetaPage tempMeta = metaPage_;
    bool needUpdateMeta = dirtyPages_.size() > 0;
    for (auto pageIndex : dirtyPages_) {
        tempMeta.bitmap->Set(pageIndex);
    }
    if (needUpdateMeta) {
        CSErrorCode errorCode = updateMetaPage(&tempMeta);
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Update metapage failed."
                        << "ChunkID: " << chunkId_
                        << ",chunk sn: " << metaPage_.sn;
            return errorCode;
        }
        metaPage_.bitmap = tempMeta.bitmap;
        dirtyPages_.clear();
    }
    return CSErrorCode::Success;
}

CSErrorCode CSSnapshot::updateMetaPage(SnapshotMetaPage* metaPage) {
    char buf[pageSize_];  // NOLINT
    memset(buf, 0, sizeof(buf));
    metaPage->encode(buf);
    int rc = writeMetaPage(buf);
    if (rc < 0) {
        LOG(ERROR) << "Update metapage failed."
                   << "ChunkID: " << chunkId_
                   << ",snapshot sn: " << metaPage_.sn;
        return CSErrorCode::InternalError;
    }
    return CSErrorCode::Success;
}

CSErrorCode CSSnapshot::loadMetaPage() {
    char buf[pageSize_];  // NOLINT
    memset(buf, 0, sizeof(buf));
    int rc = readMetaPage(buf);
    if (rc < 0) {
        LOG(ERROR) << "Error occured when reading metaPage_."
                   << " filepath = " << path();
        return CSErrorCode::InternalError;
    }
    return metaPage_.decode(buf);
}

CSSnapshots::~CSSnapshots() {
    for (auto it = snapshots_.begin(); it != snapshots_.end(); ++it) {
        delete it->second;
    }
    snapshots_.clear();
}

void CSSnapshots::insert(CSSnapshot* s) {
    if (snapshots_.count(s->GetSn()) == 0) 
        snapshots_[s->GetSn()] = s;
}

CSSnapshot *CSSnapshots::pop(SequenceNum sn) {
    if (snapshots_.count(sn) > 0) {
        CSSnapshot* snap = snapshots_[sn];
        snapshots_.erase(sn);
        return snap;
    }

    return nullptr;
}

bool CSSnapshots::contains(SequenceNum sn) const {
    return snapshots_.count(sn) > 0 ;
}

CSSnapshot* CSSnapshots::get(SequenceNum sn) {
    if (snapshots_.count(sn) > 0) {
        return snapshots_[sn];
    }

    return nullptr;
}

SequenceNum CSSnapshots::getCurrentSnapSn(SequenceNum sn, std::shared_ptr<SnapContext> ctx) const {
    if (snapshots_.empty()) {
        return 0;
    }
    // there will be snapshot which resides in snapshots_ but not in ctx (when 
    // snapshot is under process of deleting), so we find current snapshot within 
    // the following range instead of snaps array of ctx cloneFileInfo.
    bool bFindWorkingFile = false;
    SequenceNum id = ctx->getCloneFileInfo(sn).id();
    for (auto iter = snapshots_.rbegin(); iter != snapshots_.rend(); ++iter) {
        if (iter->second->GetCloneFileId() == id) {
            if (!bFindWorkingFile) {
                bFindWorkingFile = true;
                continue;
            }
            return iter->first;
        }
    }

    return 0;
}

CSSnapshot* CSSnapshots::getCurrentSnapshot(SequenceNum sn, std::shared_ptr<SnapContext> ctx) {
    SequenceNum curSnapSn = getCurrentSnapSn(sn, ctx);
    if (curSnapSn == 0) {
        return nullptr;
    }
    return snapshots_[curSnapSn];
}

CSSnapshot* CSSnapshots::getCurrentFile(SequenceNum sn, std::shared_ptr<SnapContext> ctx) {
    if (snapshots_.empty()) {
        return nullptr;
    }
    // Considering ctx maybe outdated in snapshot deletion situation, we get current file by 
    // finding largest exsiting snapshot within the clone file. We judge if the existing snapshot 
    // is within the same clone file by comparing id.
    SequenceNum id = ctx->getCloneFileInfo(sn).id();
    for (auto iter = snapshots_.rbegin(); iter != snapshots_.rend(); ++iter) {
        if (iter->second->GetCloneFileId() == id) {
            return iter->second;
        }
    }
    return nullptr;
}

CSSnapshot* CSSnapshots::getLatestFile() {
    if (snapshots_.empty()) {
        return nullptr;
    }
    return snapshots_.rbegin()->second;
}
/**
 * Assuming timeline of snapshots, from older to newer:
 *   prev -> curr -> next
*/
CSErrorCode CSSnapshots::Delete(CSChunkFile* chunkf, SequenceNum snapSn, std::shared_ptr<SnapContext> ctx) {
    // snapshot chunk not exists on disk.
    if (!this->contains(snapSn)) {
        return CSErrorCode::Success;
    }

    CSErrorCode errorCode;
    SequenceNum prev = ctx->getPrev(snapSn);
    if (prev == 0) {
        // snapSn is oldest snapshot.  Delete snap chunk directly.
        std::shared_ptr<CSSnapshot> snapshot(pop(snapSn));
        errorCode = snapshot->Delete();
        if (errorCode != CSErrorCode::Success) {
            LOG(ERROR) << "Delete snapshot failed."
                    << "ChunkID: " << snapshot->chunkId_
                    << ",snapshot sn: " << snapshot->GetSn();
            return errorCode;
        }
        return CSErrorCode::Success;
    }

    // we need to merge data from current snap chunk to `prev' snap chunk.
    if (!this->contains(prev)) {
        // No cow was generated for snap `prev', set current as `prev'.
        errorCode = Move(snapSn, prev);
        if (errorCode != CSErrorCode::Success) {
            return errorCode;
        }

        return chunkf->loadSnapshot(prev);
    }

    // do merge if both snap chunk `prev' and `curr' exists.
    return Merge(snapSn, prev);
}

/**
 * The precondition of deleting working chunk is no other snapshot chunk left
 * within this clone file. 
 * The working chunk is only deleted in the situation where file is rollbacked
 * to a new clone file and the old clone file is deleted.
*/
CSErrorCode CSSnapshots::DeleteWorkingChunk(SequenceNum sn, std::shared_ptr<SnapContext> ctx) {
    SequenceNum id = ctx->getCloneFileInfo(sn).id();
    for (auto iter = snapshots_.rbegin(); iter != snapshots_.rend(); ++iter) {
        if (iter->second->GetCloneFileId() == id) {
            std::shared_ptr<CSSnapshot> snapshot(pop(iter->second->GetSn()));
            CSErrorCode errorCode = snapshot->Delete();
            if (errorCode != CSErrorCode::Success) {
                LOG(ERROR) << "DeleteWorkingChunk failed."
                           << " ChunkID: " << snapshot->chunkId_
                           << ", sn: " << snapshot->GetSn();
                return errorCode;
            }  
            return CSErrorCode::Success;
        }
    }
    return CSErrorCode::Success;
}

/**
 * This method searchs chunk in different clone file recursively.
 * The first clone file to search is where the 'sn' belongs to, then
 * the next clone file is where the first clone file rollbacked from,
 * and so on until all pages hit.
 * If no snapshot exists through the entire read path, return ChunkNotExistError.
 * This may happen in scenarios where the snapshot was deleted by rollback
 * and no write happens to create chunk snapshot file.
 * 
 * Within a certain clone file, the chunk to be read may meet the 
 * following conditions:
 * 1. no chunk file exists
 * 2. chunk files with sequence num not less than 'sn' exist
 * 3. only chunk files with sequence num less than 'sn' exist
*/
CSErrorCode CSSnapshots::Read(SequenceNum sn, char * buf, off_t offset, size_t length, std::shared_ptr<SnapContext> ctx) {
    SequenceNum curSn = sn;
    bool isSnapshotExist = false;
    uint32_t pageBeginIndex = offset / pageSize_;
    uint32_t pageEndIndex = (offset + length - 1) / pageSize_;
    // record pages which have been visited
    std::unique_ptr<Bitmap> snapBitmap(new Bitmap(pageEndIndex+1));
    do {
        // Step1: get clone file related with sn, according to ctx
        CloneFileInfo clone = ctx->getCloneFileInfo(curSn);
        CSSnapshot* workingFile = getCurrentFile(curSn, ctx);
        if (workingFile == nullptr) {
            // there exists no snapshots within the clone file
            curSn = clone.recoversource();
            continue;
        }
        isSnapshotExist = true;
        std::vector<CSSnapshot*> snapshots;
        if (workingFile->GetSn() >= curSn) {
            // Step2: iterate over existing snapshots not less than sn within this clone file,
            //        in an ascending order by snapshot seqnum.
            for (auto iter = snapshots_.begin(); iter != snapshots_.end(); ++iter) {
                if (iter->second->GetCloneFileId() == clone.id() && iter->first >= curSn) {
                    snapshots.push_back(iter->second);
                }
            }
        } else {
            // Step3: if no result found in Step2, read the first existing snapshot
            //        less than sn within this clone file, i.e. the working chunk.
            //        This means no COW happened after the creation of snapshot.
            snapshots.push_back(workingFile);
        }

        // Step4: start to iterate over the certain snapshots until all pages hit
        for (auto snapshot: snapshots) {
            const auto it = snapshot->GetPageStatus();
            std::unique_ptr<Bitmap> curBitmap(new Bitmap(pageEndIndex+1));
            for (uint32_t i = pageBeginIndex; i <= pageEndIndex; i++) {
                if (!it->Test(i)) continue;  // page not in current COW
                if (snapBitmap->Test(i)) continue; // page already hit in previous COW

                curBitmap->Set(i);  // current copy have to read those set in `curBitmap'
                snapBitmap->Set(i); // further copy must ignore those set in `snapBitmap'
            }

            std::vector<BitRange> copiedRange;
            curBitmap->Divide(pageBeginIndex,
                            pageEndIndex,
                            nullptr,
                            &copiedRange);
            CSErrorCode errorCode = snapshot->ReadRanges(buf, offset, length, copiedRange);
            if (errorCode != CSErrorCode::Success) {
                return errorCode;
            }
            // all pages hit thus no need to iterator further
            if (snapBitmap->NextClearBit(pageBeginIndex, pageEndIndex) == Bitmap::NO_POS) {
                return CSErrorCode::Success;
            }
        }
        // Step5: start to read from clone file where it is recovered from
        curSn = clone.recoversource();
    } while (curSn > 0); // loop until the first clone file

    if (!isSnapshotExist) {
        return CSErrorCode::ChunkNotExistError;
    }
    return CSErrorCode::Success;
}

/**
 * It moves snap chunk `from` to `to', and erase `from' from `snapshots' map.
 * The caller should load snapshot `to' upon success.
 *
 * Snapshot `to' should have no COW in disk.
*/
CSErrorCode CSSnapshots::Move(SequenceNum from, SequenceNum to) {
    std::shared_ptr<CSSnapshot> snapFrom(pop(from));
    snapFrom->metaPage_.sn = to;
    CSErrorCode errorCode = snapFrom->updateMetaPage(&snapFrom->metaPage_);
    if (errorCode != CSErrorCode::Success) {
        LOG(ERROR) << "Move snapshot failed: " << errorCode;
        return errorCode;
    }

    int rc = snapFrom->lfs_->Rename(snapFrom->path(from), snapFrom->path(to), RENAME_NOREPLACE);
    if (rc != 0) {
        LOG(ERROR) << "Rename snap chunk " << snapFrom->path(from)
                   << " to " << snapFrom->path(to) << " failed: " << rc;
        return CSErrorCode::InternalError;
    }

    return CSErrorCode::Success;
}

/**
 * It merges snap chunk `from' to `to', if target bitmap is not set, and erase
 * `from' from `snapshots' map, afterwards delete the snap chunk in disk.
 *
 * Both snapshot `from' and `to' have COW in disk.
*/
CSErrorCode CSSnapshots::Merge(SequenceNum from, SequenceNum to) {
    std::shared_ptr<CSSnapshot> snapFrom(pop(from));
    CSSnapshot* snapTo = snapshots_[to];

    uint32_t pos = 0;
    char buf[pageSize_];
    // TODO read/write in ranges
    for (pos = snapFrom->metaPage_.bitmap->NextSetBit(pos); pos != Bitmap::NO_POS; pos =snapFrom->metaPage_.bitmap->NextSetBit(pos+1)) {
        if (!snapTo->metaPage_.bitmap->Test(pos)) {
            off_t offset = pageSize_ * pos;
            int rc = snapFrom->readData(buf, offset, pageSize_);
            if (rc < 0) {
                LOG(ERROR) << "Read snap chunk file failed. "
                           << "ChunkID: " << snapFrom->chunkId_
                           << ", snap sn: " << from;
                return CSErrorCode::InternalError;
            }

            rc = snapTo->Write(buf, offset, pageSize_);
            if (rc < 0) {
                LOG(ERROR) << "Write snap chunk file failed. "
                           << "ChunkID: " << snapFrom->chunkId_
                           << ", snap sn: " << to;
                return CSErrorCode::InternalError;
            }
        }
    }

    CSErrorCode errorCode = snapTo->Flush();
    if (errorCode != CSErrorCode::Success) {
        LOG(ERROR) << "Merge snapshot failed upon flush: " << errorCode;
        return errorCode;
    }

    errorCode = snapFrom->Delete();
    if (errorCode != CSErrorCode::Success) {
        LOG(ERROR) << "Delete snapshot failed."
                   << "ChunkID: " << snapFrom->chunkId_
                   << ", snap sn: " << from;
        return errorCode;
    }

    return CSErrorCode::Success;
}

/**
 * Sync all existing snapshot file.
 * This method will be called when enableOdsyncWhenOpenChunkFile_ is false.
*/
CSErrorCode CSSnapshots::Sync() {
    for (auto& pair : snapshots_) {
        CSErrorCode rc = pair.second->Sync();
        if (rc != CSErrorCode::Success) {
            return rc;
        }
    }
    return CSErrorCode::Success;
}

/**
 * Delete all existing snapshot file.
 * This method will be called when chunk is to be deleted.
*/
CSErrorCode CSSnapshots::DeleteAll() {
    for (auto& pair : snapshots_) {
        CSErrorCode rc = pair.second->Delete();
        if (rc != CSErrorCode::Success) {
            return rc;
        }
        delete pair.second;
        pair.second = nullptr;
    }
    snapshots_.clear();
    return CSErrorCode::Success;
}

}  // namespace chunkserver
}  // namespace curve
