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
 * File Created: Thursday, 6th September 2018 10:49:30 am
 * Author: yangyaokai
 */
#ifndef SRC_CHUNKSERVER_DATASTORE_CHUNKSERVER_CHUNKFILE_H_
#define SRC_CHUNKSERVER_DATASTORE_CHUNKSERVER_CHUNKFILE_H_

#include <glog/logging.h>
#include <butil/iobuf.h>
#include <string>
#include <vector>
#include <set>
#include <atomic>
#include <functional>
#include <memory>

#include "include/curve_compiler_specific.h"
#include "include/chunkserver/chunkserver_common.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/crc32.h"
#include "src/fs/local_filesystem.h"
#include "src/chunkserver/datastore/filename_operator.h"
#include "src/chunkserver/datastore/chunkserver_snapshot.h"
#include "src/chunkserver/datastore/define.h"
#include "src/chunkserver/datastore/file_pool.h"

#include "src/common/fast_align.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;
using curve::common::RWLock;
using curve::common::WriteLockGuard;
using curve::common::ReadLockGuard;
using curve::common::BitRange;

class FilePool;
class CSSnapshot;
struct DataStoreMetric;

/**
 * Chunkfile Metapage Format
 * version: 1 byte
 * sn: 8 bytes
 * correctedSn: 8 bytes
 * crc: 4 bytes
 * padding: 4075 bytes
 */
struct ChunkFileMetaPage {
    // File format version
    uint8_t version;
    // The sequence number of the chunk file
    SequenceNum sn;
    // The revised sequence number of the chunk
    SequenceNum correctedSn;
    // Indicates the location information of the data source,
    // if it is not CloneChunk, it is empty
    string location;
    // Indicates the state of the page in the current Chunk,
    // if it is not CloneChunk, it is nullptr
    std::shared_ptr<Bitmap> bitmap;

    ChunkFileMetaPage() : version(FORMAT_VERSION)
                        , sn(0)
                        , correctedSn(0)
                        , location("")
                        , bitmap(nullptr) {}
    ChunkFileMetaPage(const ChunkFileMetaPage& metaPage);
    ChunkFileMetaPage& operator = (const ChunkFileMetaPage& metaPage);

    void encode(char* buf);
    CSErrorCode decode(const char* buf);
};

struct ChunkOptions {
    // The id of the chunk, used as the file name of the chunk
    ChunkID         id;
    // The sequence number of the chunk
    SequenceNum     sn;
    // The corrected sequence number of the chunk
    SequenceNum     correctedSn;
    // Id of the clone file where this chunk belongs to
    SequenceNum     cloneFileId;
    // The directory where the chunk is located
    std::string     baseDir;
    // If you want to create a CloneChunk, need to specify this parameter to
    // indicate the location of the data source
    std::string     location;
    // chunk size
    ChunkSizeType   chunkSize;
    // The size of the page, each bit in the bitmap represents 1 page,
    // and the size of the metapage is also 1 page
    PageSizeType    pageSize;
    // enable O_DSYNC When Open ChunkFile
    bool enableOdsyncWhenOpenChunkFile;
    // datastore internal statistical metric
    std::shared_ptr<DataStoreMetric> metric;

    ChunkOptions() : id(0)
                   , sn(0)
                   , cloneFileId(0)
                   , correctedSn(0)
                   , baseDir("")
                   , location("")
                   , chunkSize(0)
                   , pageSize(0)
                   , metric(nullptr) {}
};

class CSChunkFile {
 public:
    friend class CSSnapshots;
 public:
    CSChunkFile(std::shared_ptr<LocalFileSystem> lfs,
                std::shared_ptr<FilePool> chunkFilePool,
                const ChunkOptions& options);
    virtual ~CSChunkFile();

    /**
     * When a Chunk file is created, this
     * interface will be called to initialize the Chunk file
     * Normally, there is no concurrency, mutually exclusive with other
     * operations, add write lock
     * @createFile: true means to create a new file, false not to create a
     *              file
     * @return returns the error code
     */
    CSErrorCode Open(bool createFile);
    /**
     * Called when a snapshot file is found during Datastore initialization
     * Load the metapage of the snapshot file into the memory inside the
     * function.
     * Under normal circumstances, there is no concurrency, mutually exclusive
     * with other operations, add write lock.
     * @param sn: the sequence number of the snapshot file to be loaded
     * @return: return error code
     */
    CSErrorCode LoadSnapshot(SequenceNum sn);
    /**
     * Write chunk files
     * The Write interface is called when raft apply, and there is no multiple
     * concurrency between Writes.
     * But it may be concurrent with other operations such as Read and Delete,
     * add write lock
     * @param sn: The file sequence number of the current write request
     * @param buf: data requested to be written
     * @param offset: The offset position of the request to write
     * @param length: The length of the data requested to be written
     * @param cost: The actual number of IOs generated by this request,
     * used for QOS control
     * @param ctx: The SnapContext of this chunk
     * @return: return error code
     */
    CSErrorCode Write(SequenceNum sn,
                      const butil::IOBuf& buf,
                      off_t offset,
                      size_t length,
                      uint32_t* cost,
                      std::shared_ptr<SnapContext> ctx = nullptr);

    CSErrorCode Sync();

    /**
     * Write the copied data into Chunk
     * Only write areas that have not been written, and will not overwrite
     * areas that have been written
     * There may be concurrency, add write lock
     * @param buf: request Paste data
     * @param offset: the starting offset of the data requesting Paste
     * @param length: the length of the data requested for Paste
     * @return: return error code
     */
    CSErrorCode Paste(const char * buf, off_t offset, size_t length);
    /**
     * Read chunk files
     * There may be concurrency, add read lock
     * @param buf: the data read
     * @param offset: the starting offset of the data requested to be read
     * @param length: The length of the data requested to be read
     * @return: return error code
     */
    CSErrorCode Read(char * buf, off_t offset, size_t length);
    /**
     * Read the chunk of the specified Sequence
     * There may be concurrency, add read lock
     * @param sn: SequenceNum of the specified chunk
     * @param buf: Snapshot data read
     * @param offset: the starting offset of the snapshot data requested to be
     *                read
     * @param length: The length of the snapshot data requested to be read
     * @param ctx: The SnapContext of this chunk
     * @return: return error code
     */
    CSErrorCode ReadSpecifiedChunk(SequenceNum sn,
                                   char * buf,
                                   off_t offset,
                                   size_t length,
                                   std::shared_ptr<SnapContext> ctx);
    /**
     * Delete chunk files.
     * Normally there is no concurrency, mutually exclusive with other
     * operations, add write lock.
     * @param: The file sequence number when calling the DeleteChunk interface
     * @return: return error code
     */
    CSErrorCode Delete(SequenceNum sn);
    /**
     * Delete snapshots 
     * Normally there is no concurrency, mutually exclusive with other
     * operations, add write lock.
     * @param snapSn: the sequence number that needs to be deleted
     * @param ctx: the SnapContext of file which describes the file structure
     * @return: return error code
     */
    CSErrorCode DeleteSnapshot(SequenceNum snapSn, std::shared_ptr<SnapContext> ctx = nullptr);
    /**
     * Get chunk info
     * @param[out]: the chunk info getted
     */
    void GetInfo(CSChunkInfo* info);
    /**
     * Get the hash value of the chunk, this interface is used for test
     * @param[out]: chunk hash value
     * @return: error code
     */
    CSErrorCode GetHash(off_t offset,
                        size_t length,
                        std::string *hash);

 private:
     /**
     * Called when a snapshot file is deleted and merged to a non-existent snapshot file.
     * Write lock should NOT be added
     * @param sn: the sequence number of the snapshot file to be loaded
     * @return: return error code
     */
    CSErrorCode loadSnapshot(SequenceNum sn);
    /**
     * Determine whether you need to create a new snapshot
     * @param sn: write request sequence number
     * @param ctx: The SnapContext of this chunk
     * @return: true means to create a snapshot;
     *          false means no need to create a snapshot
     */
    bool needCreateSnapshot(SequenceNum sn, std::shared_ptr<SnapContext> ctx);
    /**
     * To create a snapshot chunk
     * @param sn: sequence number of the snapshot
     * @param cloneFileId: id of the clone file where the snapshot chunk belongs to
     */
    CSErrorCode createSnapshot(SequenceNum sn, SequenceNum cloneFileId);
    /**
     * Determine whether to copy on write
     * @param sn: write request sequence number
     * @param ctx: The SnapContext of this chunk
     * @return: true means cow is required; false means cow is not required
     */
    bool needCow(SequenceNum sn, std::shared_ptr<SnapContext> ctx);
    /**
     * Copy the uncopied data in the specified area from the chunk file
     * to the snapshot file
     * @param offset: the starting offset of the write data area
     * @param length: the length of the write data area
     * @param sn: write request sequence number
     * @param ctx: The SnapContext of this chunk
     * @return: return error code
     */
    CSErrorCode copy2Snapshot(off_t offset, size_t length, SequenceNum sn, std::shared_ptr<SnapContext> ctx);
    /**
     * Find working chunk in the current clone file, and create one if not exists.
     * "working chunk" is the target of write/read chunk request and it may not exist
     * after rollback until write chunk
     * @param sn: The file sequence number of the current write request
     * @param ctx: The SnapContext of this chunk
     * @return: return error code
    */
   CSErrorCode createChunkIfNotExist(SequenceNum sn, std::shared_ptr<SnapContext> ctx);

    inline bool CheckOffsetAndLength(off_t offset, size_t len, size_t align) {
        // Check if offset+len is out of bounds
        if (offset + len > size_) {
            return false;
        }

        return common::is_aligned(offset, align) &&
               common::is_aligned(len, align);
    }

 private:
    // The logical size of the chunk, not including metapage
    ChunkSizeType size_;
    // The smallest atomic read and write unit
    PageSizeType pageSize_;
    // chunk id
    ChunkID chunkId_;
    // The directory where the chunk is located
    std::string baseDir_;
    // read-write lock
    RWLock rwLock_;
    // Snapshot file pointer
    std::shared_ptr<CSSnapshots> snapshots_;
    // initial seqNum when this CSChunkFile is constructed
    SequenceNum sn_;
    // initial cloneFileId when this CSChunkFile is constructed
    SequenceNum cloneFileId_;
    // Rely on FilePool to create and delete files
    std::shared_ptr<FilePool> chunkFilePool_;
    // Rely on the local file system to manipulate files
    std::shared_ptr<LocalFileSystem> lfs_;
    // datastore internal statistical indicators
    std::shared_ptr<DataStoreMetric> metric_;
    // enable O_DSYNC When Open ChunkFile
    bool enableOdsyncWhenOpenChunkFile_;
};
}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_DATASTORE_CHUNKSERVER_CHUNKFILE_H_
