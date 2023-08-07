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
 * Created Date: Tuesday December 4th 2018
 * Author: yangyaokai
 */

#ifndef SRC_CHUNKSERVER_DATASTORE_DEFINE_H_
#define SRC_CHUNKSERVER_DATASTORE_DEFINE_H_

#include <string>
#include <memory>

#include "include/chunkserver/chunkserver_common.h"
#include "src/common/bitmap.h"

namespace curve {
namespace chunkserver {

class ChunkRequest;

using curve::common::Bitmap;

// const uint8_t FORMAT_VERSION = 1;
const uint8_t FORMAT_VERSION = 2;  // format for instant rollback
const SequenceNum kInvalidSeq = 0;

DECLARE_uint32(minIoAlignment);

// define error code
enum CSErrorCode {
    // success
    Success = 0,
    // Internal error, usually an error during system call
    InternalError = 1,
    // Version is not compatible
    IncompatibleError = 2,
    // crc verification failed
    CrcCheckError = 3,
    // The file format is incorrect, for example the file length is incorrect
    FileFormatError = 4,
    // Snapshot conflict, there are multiple snapshot files
    SnapshotConflictError = 5,
    // For requests with outdated sequences, it is normal if the error is thrown
    // during log recovery
    BackwardRequestError = 6,
    // Thrown when there is a snapshot file when deleting a chunk, it is not
    // allowed to delete a chunk with a snapshot
    SnapshotExistError = 7,
    // The chunk requested to read and write does not exist
    ChunkNotExistError = 8,
    // The area requested to read and write exceeds the size of the file
    OutOfRangeError = 9,
    // Parameter error
    InvalidArgError = 10,
    // There are conflicting chunks when creating chunks
    ChunkConflictError = 11,
    // Status conflict
    StatusConflictError = 12,
    // The page has not been written, it will appear when the page that has not
    // been written is read when the clone chunk is read
    PageNerverWrittenError = 13,
    // Thrown when given snapshot is not found for a chunk.
    SnapshotNotExistError = 14,
};

// Chunk details
struct CSChunkInfo {
    // the id of the chunk
    ChunkID chunkId;
    // page size
    uint32_t pageSize;
    // The size of the chunk
    uint32_t chunkSize;
    // The sequence number of the chunk file
    SequenceNum curSn;
    // The sequence number of the chunk snapshot,
    // if the snapshot does not exist, it is 0
    SequenceNum snapSn;
    // The revised sequence number of the chunk
    SequenceNum correctedSn;
    // Indicates whether the chunk is CloneChunk
    bool isClone;
    // If it is CloneChunk, it indicates the location of the data source;
    // otherwise it is empty
    std::string location;
    // If it is CloneChunk, it means the state of the current Chunk page,
    // otherwise it is nullptr
    std::shared_ptr<Bitmap> bitmap;
    CSChunkInfo() : chunkId(0)
                  , pageSize(4096)
                  , chunkSize(16 * 4096 * 4096)
                  , curSn(0)
                  , isClone(false)
                  , location("")
                  , bitmap(nullptr) {}

    bool operator== (const CSChunkInfo& rhs) const {
        if (chunkId != rhs.chunkId ||
            pageSize != rhs.pageSize ||
            chunkSize != rhs.chunkSize ||
            curSn != rhs.curSn ||
            isClone != rhs.isClone ||
            location != rhs.location) {
            return false;
        }
        // If the bitmap is not nullptr, compare whether the contents are equal
        if (bitmap != nullptr && rhs.bitmap != nullptr) {
            if (*bitmap != *rhs.bitmap)
                return false;
        } else {
            // Determine whether both are nullptr
            if (bitmap != rhs.bitmap)
                return false;
        }

        return true;
    }

    bool operator!= (const CSChunkInfo& rhs) const {
        return !(*this == rhs);
    }
};



class SnapContext {
 public:
    SnapContext(const CloneFileInfos& cloneFileInfos);
    virtual ~SnapContext() = default;

    static std::shared_ptr<SnapContext> build_empty(SequenceNum sn) {
        CloneFileInfos infos;
        infos.set_seqnum(sn);
        CloneFileInfo info;
        info.set_id(1);
        info.set_seqnum(sn);
        info.set_recoversource(0);
        auto clone = infos.add_clones();
        clone->CopyFrom(info);
        return std::make_shared<SnapContext>(infos);
    }

    /**
     * Get first snapshot sequence num smaller than sn within the specified clone file
     * @param sn: the sequence num to compare and to specify the clone file
     * @return: return the result snap
    */    
    SequenceNum getPrev(SequenceNum sn) const;
    /**
     * Get latest snapshot sequence num within the specified clone file
     * @param sn: the sequence num to specify the clone file
     * @return: return the result snap
    */    
    SequenceNum getLatest(SequenceNum sn) const;
    /**
     * Judge if a sequence num is contained in the specified clone file
     * @param sn: the sequence num to judge
     * @return: return the judge result 
    */  
    bool contains(SequenceNum sn) const;
    /**
     * Judge if the specified clone file has snaps
     * @param sn: the sequence num to specify the clone file
     * @return: return the judge result 
    */  
    bool empty(SequenceNum sn) const;
    /**
     * Get the clone file where the sequence num belongs to,
     * or return the default empty clone file if not found
     * @param sn: the sequence num to check
     * @return: return the clone file info
    */    
    CloneFileInfo getCloneFileInfo(SequenceNum sn) const;
    /**
     * Get the clone file infos, mainly for debugging
     * @return: return the clone file infos
    */    
    CloneFileInfos getCloneFileInfos() const;
    /**
     * Get the sequence num of the current working file, i.e. the
     * maximum sequence num
     * @return: return the result 
    */
    SequenceNum getCurrentFileSn() const;


 protected:
    SnapContext() = default;
    CloneFileInfos cloneFileInfos_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_DATASTORE_DEFINE_H_
