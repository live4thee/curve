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
 * Created Date: Friday September 7th 2018
 * Author: hzsunjianliang
 */

#include "src/mds/nameserver2/curvefs.h"
#include <glog/logging.h>
#include <memory>
#include <chrono>    //NOLINT
#include <set>
#include <utility>
#include <map>
#include "src/common/string_util.h"
#include "src/common/encode.h"
#include "src/common/timeutility.h"
#include "src/mds/nameserver2/namespace_storage.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"

using curve::common::TimeUtility;
using curve::mds::topology::LogicalPool;
using curve::mds::topology::LogicalPoolIdType;
using curve::mds::topology::CopySetIdType;
using curve::mds::topology::ChunkServer;
using curve::mds::topology::ChunkServerStatus;
using curve::mds::topology::OnlineState;

namespace curve {
namespace mds {

ClientInfo EndPointToClientInfo(const butil::EndPoint& ep) {
    ClientInfo info;
    info.set_ip(butil::ip2str(ep.ip).c_str());
    info.set_port(ep.port);

    return info;
}

bool CurveFS::InitRecycleBinDir() {
    FileInfo recyclebinFileInfo;

    StoreStatus ret = storage_->GetFile(ROOTINODEID, RECYCLEBINDIR,
        &recyclebinFileInfo);
    if (ret == StoreStatus::OK) {
        if (recyclebinFileInfo.parentid() != ROOTINODEID
            ||  recyclebinFileInfo.id() != RECYCLEBININODEID
            ||  recyclebinFileInfo.filename().compare(RECYCLEBINDIRNAME) != 0
            ||  recyclebinFileInfo.filetype() != FileType::INODE_DIRECTORY
            ||  recyclebinFileInfo.owner() != rootAuthOptions_.rootOwner) {
            LOG(ERROR) << "Recyclebin info error, fileInfo = "
                << recyclebinFileInfo.DebugString();
            return false;
        } else {
            LOG(INFO) << "Recycle Bin dir exist, Path = " << RECYCLEBINDIR;
            return true;
        }
    } else if ( ret == StoreStatus::KeyNotExist ) {
        // store the dir
        recyclebinFileInfo.set_parentid(ROOTINODEID);
        recyclebinFileInfo.set_id(RECYCLEBININODEID);
        recyclebinFileInfo.set_filename(RECYCLEBINDIRNAME);
        recyclebinFileInfo.set_filetype(FileType::INODE_DIRECTORY);
        recyclebinFileInfo.set_ctime(
            ::curve::common::TimeUtility::GetTimeofDayUs());
        recyclebinFileInfo.set_owner(rootAuthOptions_.rootOwner);

        StoreStatus ret2 = storage_->PutFile(recyclebinFileInfo);
        if ( ret2 != StoreStatus::OK ) {
            LOG(ERROR) << "RecycleBin dir create error, Path = "
                << RECYCLEBINDIR;
            return false;
        }
        LOG(INFO) << "RecycleBin dir create ok, Path = " << RECYCLEBINDIR;
        return true;
    } else {
        // internal error
        LOG(INFO) << "InitRecycleBinDir error ,ret = " << ret;
        return false;
    }
}

bool CurveFS::Init(std::shared_ptr<NameServerStorage> storage,
                std::shared_ptr<InodeIDGenerator> InodeIDGenerator,
                std::shared_ptr<ChunkSegmentAllocator> chunkSegAllocator,
                std::shared_ptr<CleanManagerInterface> cleanManager,
                std::shared_ptr<FileRecordManager> fileRecordManager,
                std::shared_ptr<AllocStatistic> allocStatistic,
                const struct CurveFSOption &curveFSOptions,
                std::shared_ptr<Topology> topology,
                std::shared_ptr<SnapshotCloneClient> snapshotCloneClient) {
    startTime_ = std::chrono::steady_clock::now();
    storage_ = storage;
    InodeIDGenerator_ = InodeIDGenerator;
    chunkSegAllocator_ = chunkSegAllocator;
    cleanManager_ = cleanManager;
    allocStatistic_ = allocStatistic;
    fileRecordManager_ = fileRecordManager;
    rootAuthOptions_ = curveFSOptions.authOptions;

    defaultChunkSize_ = curveFSOptions.defaultChunkSize;
    defaultSegmentSize_ = curveFSOptions.defaultSegmentSize;
    minFileLength_ = curveFSOptions.minFileLength;
    maxFileLength_ = curveFSOptions.maxFileLength;
    topology_ = topology;
    snapshotCloneClient_ = snapshotCloneClient;

    InitRootFile();
    bool ret = InitRecycleBinDir();
    if (!ret) {
        LOG(ERROR) << "Init RecycleBin fail!";
        return false;
    }

    fileRecordManager_->Init(curveFSOptions.fileRecordOptions);

    return true;
}

void CurveFS::Run() {
    fileRecordManager_->Start();
}

void CurveFS::Uninit() {
    fileRecordManager_->Stop();
    storage_ = nullptr;
    InodeIDGenerator_ = nullptr;
    chunkSegAllocator_ = nullptr;
    cleanManager_ = nullptr;
    allocStatistic_ = nullptr;
    fileRecordManager_ = nullptr;
    snapshotCloneClient_ = nullptr;
}

void CurveFS::InitRootFile(void) {
    rootFileInfo_.set_id(ROOTINODEID);
    rootFileInfo_.set_filename(ROOTFILENAME);
    rootFileInfo_.set_filetype(FileType::INODE_DIRECTORY);
    rootFileInfo_.set_owner(GetRootOwner());
}

StatusCode CurveFS::WalkPath(const std::string &fileName,
                        FileInfo *fileInfo, std::string  *lastEntry) const  {
    assert(lastEntry != nullptr);

    std::vector<std::string> paths;
    ::curve::common::SplitString(fileName, "/", &paths);

    if ( paths.size() == 0 ) {
        fileInfo->CopyFrom(rootFileInfo_);
        return StatusCode::kOK;
    }

    *lastEntry = paths.back();
    uint64_t parentID = rootFileInfo_.id();

    for (uint32_t i = 0; i < paths.size() - 1; i++) {
        auto ret = storage_->GetFile(parentID, paths[i], fileInfo);

        if (ret ==  StoreStatus::OK) {
            if (fileInfo->filetype() !=  FileType::INODE_DIRECTORY) {
                LOG(INFO) << fileInfo->filename() << " is not an directory";
                return StatusCode::kNotDirectory;
            }
        } else if (ret == StoreStatus::KeyNotExist) {
            return StatusCode::kFileNotExists;
        } else {
            LOG(ERROR) << "GetFile error, errcode = " << ret;
            return StatusCode::kStorageError;
        }
        // assert(fileInfo->parentid() != parentID);
        parentID =  fileInfo->id();
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::LookUpFile(const FileInfo & parentFileInfo,
                    const std::string &fileName, FileInfo *fileInfo) const {
    assert(fileInfo != nullptr);

    auto ret = storage_->GetFile(parentFileInfo.id(), fileName, fileInfo);

    if (ret == StoreStatus::OK) {
        return StatusCode::kOK;
    } else if  (ret == StoreStatus::KeyNotExist) {
        return StatusCode::kFileNotExists;
    } else {
        return StatusCode::kStorageError;
    }
}

// StatusCode PutFileInternal()

StatusCode CurveFS::PutFile(const FileInfo & fileInfo) {
    if (storage_->PutFile(fileInfo) != StoreStatus::OK) {
        return StatusCode::kStorageError;
    } else {
        return StatusCode::kOK;
    }
}

StatusCode CurveFS::SnapShotFile(const FileInfo * origFileInfo,
                                const FileInfo * snapshotFile) const {
    if (storage_->SnapShotFile(origFileInfo, snapshotFile) != StoreStatus::OK) {
        return StatusCode::kStorageError;
    } else {
        return StatusCode::kOK;
    }
}

StatusCode CurveFS::SnapShotFile(const FileInfo * origFileInfo,
                                const FileInfo * snapshotFile, 
                                const FileInfo * innerSnapshot) const {
    if (storage_->SnapShotFile(origFileInfo, snapshotFile, innerSnapshot) != StoreStatus::OK) {
        return StatusCode::kStorageError;
    } else {
        return StatusCode::kOK;
    }
}

StatusCode CurveFS::CreateFile(const std::string & fileName,
                               const std::string& owner,
                               FileType filetype, uint64_t length,
                               uint64_t stripeUnit, uint64_t stripeCount) {
    FileInfo parentFileInfo;
    std::string lastEntry;

    if (filetype != FileType::INODE_PAGEFILE
            && filetype !=  FileType::INODE_DIRECTORY) {
        LOG(ERROR) << "CreateFile not support create file type : " << filetype
                   << ", fileName = " << fileName;
        return StatusCode::kNotSupported;
    }

    // check param
    if (filetype == FileType::INODE_PAGEFILE) {
        if  (length < minFileLength_) {
            LOG(ERROR) << "file Length < MinFileLength " << minFileLength_
                       << ", length = " << length;
            return StatusCode::kFileLengthNotSupported;
        }

        if (length > maxFileLength_) {
            LOG(ERROR) << "CreateFile file length > maxFileLength, fileName = "
                       << fileName << ", length = " << length
                       << ", maxFileLength = " << maxFileLength_;
            return StatusCode::kFileLengthNotSupported;
        }

        if (length % defaultSegmentSize_ != 0) {
            LOG(ERROR) << "Create file length not align to segment size, "
                       << "fileName = " << fileName
                       << ", length = " << length
                       << ", segment size = " << defaultSegmentSize_;
            return StatusCode::kFileLengthNotSupported;
        }
    }

    auto ret = CheckStripeParam(stripeUnit, stripeCount);
    if (ret != StatusCode::kOK) {
        return ret;
    }
    ret = WalkPath(fileName, &parentFileInfo, &lastEntry);
    if ( ret != StatusCode::kOK ) {
        return ret;
    }
    if (lastEntry.empty()) {
        return StatusCode::kFileExists;
    }

    FileInfo fileInfo;
    ret = LookUpFile(parentFileInfo, lastEntry, &fileInfo);
    if (ret == StatusCode::kOK) {
        return StatusCode::kFileExists;
    }

    if (ret != StatusCode::kFileNotExists) {
         return ret;
    } else {
        InodeID inodeID;
        if ( InodeIDGenerator_->GenInodeID(&inodeID) != true ) {
            LOG(ERROR) << "GenInodeID  error";
            return StatusCode::kStorageError;
        }

        fileInfo.set_id(inodeID);
        fileInfo.set_filename(lastEntry);
        fileInfo.set_parentid(parentFileInfo.id());
        fileInfo.set_filetype(filetype);
        fileInfo.set_owner(owner);
        fileInfo.set_chunksize(defaultChunkSize_);
        fileInfo.set_segmentsize(defaultSegmentSize_);
        fileInfo.set_length(length);
        fileInfo.set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());
        fileInfo.set_seqnum(kStartSeqNum);
        fileInfo.set_filestatus(FileStatus::kFileCreated);
        fileInfo.set_stripeunit(stripeUnit);
        fileInfo.set_stripecount(stripeCount);

        // put a clone file, for the usage of instant rollback
        auto clone = fileInfo.add_clones();
        clone->set_id(1);
        clone->set_seqnum(kStartSeqNum);
        clone->set_recoversource(0);

        ret = PutFile(fileInfo);
        return ret;
    }
}

StatusCode CurveFS::GetFileInfo(const std::string & filename,
                                FileInfo *fileInfo) const {
    assert(fileInfo != nullptr);
    std::string lastEntry;
    FileInfo parentFileInfo;
    auto ret = WalkPath(filename, &parentFileInfo, &lastEntry);
    if ( ret != StatusCode::kOK ) {
        if (ret == StatusCode::kNotDirectory) {
            return StatusCode::kFileNotExists;
        }
        return ret;
    } else {
        if (lastEntry.empty()) {
            fileInfo->CopyFrom(parentFileInfo);
            return StatusCode::kOK;
        }
        return LookUpFile(parentFileInfo, lastEntry, fileInfo);
    }
}

AllocatedSize& AllocatedSize::operator+=(const AllocatedSize& rhs) {
    total += rhs.total;
    for (const auto& item : rhs.allocSizeMap) {
        allocSizeMap[item.first] += item.second;
    }
    return *this;
}

StatusCode CurveFS::GetAllocatedSize(const std::string& fileName,
                                     AllocatedSize* allocatedSize) {
    assert(allocatedSize != nullptr);
    allocatedSize->total = 0;
    allocatedSize->allocSizeMap.clear();
    FileInfo fileInfo;
    auto ret = GetFileInfo(fileName, &fileInfo);
    if (ret != StatusCode::kOK) {
        return ret;
    }

    if (fileInfo.filetype() != curve::mds::FileType::INODE_DIRECTORY &&
                fileInfo.filetype() != curve::mds::FileType::INODE_PAGEFILE) {
        LOG(ERROR) << "GetAllocatedSize not support file type : "
                   << fileInfo.filetype() << ", fileName = " << fileName;
        return StatusCode::kNotSupported;
    }

    return GetAllocatedSize(fileName, fileInfo, allocatedSize);
}

StatusCode CurveFS::GetAllocatedSize(const std::string& fileName,
                                     const FileInfo& fileInfo,
                                     AllocatedSize* allocSize) {
    if (fileInfo.filetype() != curve::mds::FileType::INODE_DIRECTORY) {
        return GetFileAllocSize(fileName, fileInfo, allocSize);
    } else {
        // for directory, calculate the size of each file recursively and sum up
        return GetDirAllocSize(fileName, fileInfo, allocSize);
    }
}

StatusCode CurveFS::GetFileAllocSize(const std::string& fileName,
                                     const FileInfo& fileInfo,
                                     AllocatedSize* allocSize) {
    std::vector<PageFileSegment> segments;
    auto listSegmentRet = storage_->ListSegment(fileInfo.id(), &segments);

    if (listSegmentRet != StoreStatus::OK) {
        return StatusCode::kStorageError;
    }
    for (const auto& segment : segments) {
        const auto & poolId = segment.logicalpoolid();
        allocSize->allocSizeMap[poolId] += fileInfo.segmentsize();
    }
    allocSize->total = fileInfo.segmentsize() * segments.size();
    return StatusCode::kOK;
}

StatusCode CurveFS::GetDirAllocSize(const std::string& fileName,
                                    const FileInfo& fileInfo,
                                    AllocatedSize* allocSize) {
    std::vector<FileInfo> files;
    StatusCode ret = ReadDir(fileName, &files);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "ReadDir Fail, fileName: " << fileName;
        return ret;
    }
    for (const auto& file : files) {
        std::string fullPathName;
        if (fileName == "/") {
            fullPathName = fileName + file.filename();
        } else {
            fullPathName = fileName + "/" + file.filename();
        }
        AllocatedSize size;
        if (GetAllocatedSize(fullPathName, file, &size) != 0) {
            std::cout << "Get allocated size of " << fullPathName
                      << " fail!" << std::endl;
            continue;
        }
        *allocSize += size;
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::GetFileSize(const std::string& fileName, uint64_t* size) {
    assert(size != nullptr);
    *size = 0;
    FileInfo fileInfo;
    auto ret = GetFileInfo(fileName, &fileInfo);
    if (ret != StatusCode::kOK) {
        return ret;
    }

    if (fileInfo.filetype() != curve::mds::FileType::INODE_DIRECTORY &&
                fileInfo.filetype() != curve::mds::FileType::INODE_PAGEFILE) {
        LOG(ERROR) << "GetFileSize not support file type : "
                   << fileInfo.filetype() << ", fileName = " << fileName;
        return StatusCode::kNotSupported;
    }
    return GetFileSize(fileName, fileInfo, size);
}

StatusCode CurveFS::GetFileSize(const std::string& fileName,
                                const FileInfo& fileInfo,
                                uint64_t* fileSize) {
    // return file length if it is a file
    switch (fileInfo.filetype()) {
        case FileType::INODE_PAGEFILE: {
            *fileSize = fileInfo.length();
            return StatusCode::kOK;
        }
        case FileType::INODE_SNAPSHOT_PAGEFILE: {
            // Do not count snapshot file size, set fileSize=0
            *fileSize = 0;
            return StatusCode::kOK;
        }
        case FileType::INODE_DIRECTORY: {
            break;
        }
        default: {
            LOG(ERROR) << "Get file size of type "
                       << FileType_Name(fileInfo.filetype())
                       << " not supported";
            return StatusCode::kNotSupported;
        }
    }
    // if it is a directory, list the dir and calculate file size recursively
    std::vector<FileInfo> files;
    StatusCode ret = ReadDir(fileName, &files);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "ReadDir Fail, fileName: " << fileName
                   << ", error code: " << ret;
        return ret;
    }
    for (auto& file : files) {
        std::string fullPathName;
        if (fileName == "/") {
            fullPathName = fileName + file.filename();
        } else {
            fullPathName = fileName + "/" + file.filename();
        }
        uint64_t size = 0;
        ret = GetFileSize(fullPathName, file, &size);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "Get file size of " << fullPathName
                       << " fail, error code: " << ret;
            return ret;
        }
        *fileSize += size;
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::isDirectoryEmpty(const FileInfo &fileInfo, bool *result) {
    assert(fileInfo.filetype() == FileType::INODE_DIRECTORY);
    std::vector<FileInfo> fileInfoList;
    auto storeStatus = storage_->ListFile(fileInfo.id(), fileInfo.id() + 1,
                                          &fileInfoList);
    if (storeStatus == StoreStatus::KeyNotExist) {
        *result = true;
        return StatusCode::kOK;
    }

    if (storeStatus != StoreStatus::OK) {
        LOG(ERROR) << "list file fail, inodeid = " << fileInfo.id()
                   << ", dir name = " << fileInfo.filename();
        return StatusCode::kStorageError;
    }

    if (fileInfoList.size() ==  0) {
        *result = true;
        return StatusCode::kOK;
    }

    *result = false;
    return StatusCode::kOK;
}

StatusCode CurveFS::IsSnapshotAllowed(const std::string &fileName) {
    if (!IsStartEnoughTime(10)) {
        LOG(INFO) << "snapshot is not allowed now, fileName = " << fileName;
        return StatusCode::kSnapshotFrozen;
    }

    // the client version satisfies the conditions
    std::string clientVersion;
    bool exist = fileRecordManager_->GetMinimumFileClientVersion(
        fileName, &clientVersion);
    if (!exist) {
        return StatusCode::kOK;
    }

    if (clientVersion < kLeastSupportSnapshotClientVersion) {
        LOG(INFO) << "current client version is not support snapshot"
                  << ", fileName = " << fileName
                  << ", clientVersion = " << clientVersion
                  << ", leastSupportSnapshotClientVersion = "
                  << kLeastSupportSnapshotClientVersion;
        return StatusCode::kClientVersionNotMatch;
    }

    return StatusCode::kOK;
}

StatusCode CurveFS::DeleteFile(const std::string & filename, uint64_t fileId,
                            bool deleteForce, bool deleteSnaps) {
    std::string lastEntry;
    FileInfo parentFileInfo;
    auto ret = WalkPath(filename, &parentFileInfo, &lastEntry);
    if ( ret != StatusCode::kOK ) {
        return ret;
    }

    if (lastEntry.empty()) {
        LOG(INFO) << "can not remove rootdir";
        return StatusCode::kParaError;
    }

    FileInfo fileInfo;
    ret = LookUpFile(parentFileInfo, lastEntry, &fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "delete file lookupfile fail, fileName = " << filename;
        return ret;
    }

    if (fileId != kUnitializedFileID && fileId != fileInfo.id()) {
        LOG(WARNING) << "delete file, file id missmatch"
                   << ", fileName = " << filename
                   << ", fileInfo.id() = " << fileInfo.id()
                   << ", para fileId = " << fileId;
        return StatusCode::kFileIdNotMatch;
    }

    if (fileInfo.filetype() == FileType::INODE_DIRECTORY) {
        // if there are still files in it, the directory cannot be deleted
        bool isEmpty = false;
        auto ret1 = isDirectoryEmpty(fileInfo, &isEmpty);
        if (ret1 != StatusCode::kOK) {
            LOG(ERROR) << "check is directory empty fail, filename = "
                       << filename << ", ret = " << ret1;
            return ret1;
        }
        if (!isEmpty) {
            LOG(WARNING) << "delete file, file is directory and not empty"
                       << ", filename = " << filename;
            return StatusCode::kDirNotEmpty;
        }
        auto ret = storage_->DeleteFile(fileInfo.parentid(),
                                                fileInfo.filename());
        if (ret != StoreStatus::OK) {
            LOG(ERROR) << "delete file, file is directory and delete fail"
                       << ", filename = " << filename
                       << ", ret = " << ret;
            return StatusCode::kStorageError;
        }

        LOG(INFO) << "delete file success, file is directory"
                  << ", filename = " << filename;
        return StatusCode::kOK;
    } else if (fileInfo.filetype() == FileType::INODE_PAGEFILE) {
        StatusCode ret = CheckFileCanDelete(filename, deleteSnaps);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "delete file, can not delete file"
                       << ", filename = " << filename
                       << ", ret = " << ret;
            return ret;
        }
        if (deleteForce == false) {
            // move the file to the recycle bin
            FileInfo recycleFileInfo;
            recycleFileInfo.CopyFrom(fileInfo);
            recycleFileInfo.set_parentid(RECYCLEBININODEID);
            recycleFileInfo.set_filename(fileInfo.filename() + "-" +
                std::to_string(recycleFileInfo.id()));
            recycleFileInfo.set_originalfullpathname(filename);

            StoreStatus ret1 =
                storage_->MoveFileToRecycle(fileInfo, recycleFileInfo);
            if (ret1 != StoreStatus::OK) {
                LOG(ERROR) << "delete file, move file to recycle fail"
                        << ", filename = " << filename
                        << ", ret = " << ret1;
                return StatusCode::kStorageError;
            }
            LOG(INFO) << "file delete to recyclebin, fileName = " << filename
                      << ", recycle filename = " << recycleFileInfo.filename();
            return StatusCode::kOK;
        } else {
            // direct removefile is not support
            if (fileInfo.parentid() != RECYCLEBININODEID) {
                LOG(WARNING)
                    << "force delete file not in recyclebin"
                    << "not support yet, filename = " << filename;
                return StatusCode::kNotSupported;
            }

            if (fileInfo.filestatus() == FileStatus::kFileDeleting) {
                LOG(INFO) << "file is underdeleting, filename = " << filename;
                return StatusCode::kFileUnderDeleting;
            }

            // check whether the task already exist
            if ( cleanManager_->GetTask(fileInfo.id()) != nullptr ) {
                LOG(WARNING) << "filename = " << filename << ", inode = "
                    << fileInfo.id() << ", deleteFile task already submited";
                return StatusCode::kOK;
            }

            fileInfo.set_filestatus(FileStatus::kFileDeleting);
            auto ret = PutFile(fileInfo);
            if (ret != StatusCode::kOK) {
                LOG(ERROR) << "delete file put deleting file fail, filename = "
                           << filename << ", retCode = " << ret;
                return StatusCode::KInternalError;
            }

            // submit a file deletion task
            if (!cleanManager_->SubmitDeleteCommonFileJob(fileInfo)) {
                LOG(ERROR) << "fileName = " << filename
                        << ", inode = " << fileInfo.id()
                        << ", submit delete file job fail.";
                return StatusCode::KInternalError;
            }

            LOG(INFO) << "delete file task submitted, file is pagefile"
                        << ", inode = " << fileInfo.id()
                        << ", filename = " << filename;
            return StatusCode::kOK;
        }
     } else {
        // Currently deletefile only supports the deletion of INODE_DIRECTORY
        // and INODE_PAGEFILE type files.
        // INODE_SNAPSHOT_PAGEFILE is not deleted by this interface to delete.
        // Other file types are temporarily not supported.
        LOG(ERROR) << "delete file fail, file type not support delete"
                   << ", filename = " << filename
                   << ", fileType = " << fileInfo.filetype();
        return kNotSupported;
    }
}


StatusCode CurveFS::IncreaseFileEpoch(const std::string &filename,
    FileInfo *fileInfo,
    ::google::protobuf::RepeatedPtrField<ChunkServerLocation> *cslocs) {
    assert(fileInfo != nullptr);
    assert(cslocs != nullptr);
    std::string lastEntry;
    FileInfo parentFileInfo;
    auto ret = WalkPath(filename, &parentFileInfo, &lastEntry);
    if (ret != StatusCode::kOK) {
        if (ret == StatusCode::kNotDirectory) {
            return StatusCode::kFileNotExists;
        }
        return ret;
    } else {
        if (lastEntry.empty()) {
            LOG(ERROR) << "IncreaseFileEpoch found a directory"
                       << ", filename: " << filename;
            return StatusCode::kParaError;
        }
        ret = LookUpFile(parentFileInfo, lastEntry, fileInfo);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "LookUpFile failed, ret: " << ret
                       << ", filename: " << filename;
            return ret;
        }
        if (fileInfo->has_epoch()) {
            uint64_t old = fileInfo->epoch();
            fileInfo->set_epoch(++old);
        } else {
            fileInfo->set_epoch(1);
        }
        ret = PutFile(*fileInfo);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "Put File faied, ret: " << ret
                       << ", filename: " << filename;
            return ret;
        }

        // add chunkserver locations
        std::vector<ChunkServerIdType> chunkserverlist =
            topology_->GetChunkServerInCluster(
                [] (const ChunkServer &cs) {
                    return cs.GetStatus() != ChunkServerStatus::RETIRED;
                });
        for (auto &id : chunkserverlist) {
            ChunkServer cs;
            if (topology_->GetChunkServer(id, &cs)) {
                ChunkServerLocation *lc = cslocs->Add();
                lc->set_chunkserverid(id);
                lc->set_hostip(cs.GetHostIp());
                lc->set_port(cs.GetPort());
                lc->set_externalip(cs.GetExternalHostIp());
            }
        }
        return StatusCode::kOK;
    }
}

// TODO(hzsunjianliang): CheckNormalFileDeleteStatus?

StatusCode CurveFS::ReadDir(const std::string & dirname,
                            std::vector<FileInfo> * files) const {
    assert(files != nullptr);

    FileInfo fileInfo;
    auto ret = GetFileInfo(dirname, &fileInfo);
    if (ret != StatusCode::kOK) {
        if ( ret == StatusCode::kFileNotExists ) {
            return StatusCode::kDirNotExist;
        }
        return ret;
    }

    if (fileInfo.filetype() != FileType::INODE_DIRECTORY) {
        return StatusCode::kNotDirectory;
    }

    if (storage_->ListFile(fileInfo.id(),
                            fileInfo.id() + 1,
                            files) != StoreStatus::OK ) {
        return StatusCode::kStorageError;
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::CheckFileCanChange(const std::string &fileName,
    const FileInfo &fileInfo) {
    // Check if the file has a snapshot
    std::vector<FileInfo> snapshotFileInfos;
    auto ret = ListSnapShotFile(fileName, &snapshotFileInfos);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "CheckFileCanChange, list snapshot file fail"
                    << ", fileName = " << fileName;
        return ret;
    }

    if (snapshotFileInfos.size() != 0) {
        LOG(WARNING) << "CheckFileCanChange, file is under snapshot, "
                     << "cannot delete or rename, fileName = " << fileName;
        return StatusCode::kFileUnderSnapShot;
    }

    if (fileInfo.filestatus() == FileStatus::kFileBeingCloned) {
        LOG(WARNING) << "CheckFileCanChange, file is being Cloned, "
                   << "need check first, fileName = " << fileName;
        return StatusCode::kDeleteFileBeingCloned;
    }

    // since the file record is not persistent, after mds switching the leader,
    // file record manager is empty
    // after file is opened, there will be refresh session requests in each
    // file record expiration time
    // so wait for a file record expiration time to make sure that
    // the file record is updated
    if (!IsStartEnoughTime(1)) {
        LOG(WARNING) << "MDS doesn't start enough time";
        return StatusCode::kNotSupported;
    }

    std::vector<butil::EndPoint> endPoints;
    if (fileRecordManager_->FindFileMountPoint(fileName, &endPoints) &&
        !endPoints.empty()) {
        LOG(WARNING) << fileName << " has " << endPoints.size()
                     << " mount points";
        return StatusCode::kFileOccupied;
    }

    return StatusCode::kOK;
}

StatusCode CurveFS::CheckFileCanDelete(const std::string &fileName, bool deleteSnaps) {
    // Check if the file has a snapshot
    std::vector<FileInfo> snapshotFileInfos;
    auto ret = ListSnapShotFile(fileName, &snapshotFileInfos);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "CheckFileCanDelete, list snapshot file fail"
                    << ", fileName = " << fileName;
        return ret;
    }

    // If the snapshot is under deleting or to be deleted,
    // it has no reason to prevent the file from deletion 
    size_t existingSnapNum = 0;
    for (const FileInfo& snap : snapshotFileInfos) {
        if (snap.filestatus() == FileStatus::kFileCreated) {
            existingSnapNum++;
        }
    }

    if (existingSnapNum > 0) {
        if (!deleteSnaps) {
            LOG(WARNING) << "CheckFileCanDelete, file is under snapshot "
                        << " and deleteSnaps is false, "
                        << "cannot delete fileName = " << fileName;
            return StatusCode::kFileUnderSnapShot;
        } else {
            LOG(INFO) << "File is checked to be deleted with " << existingSnapNum
                      << " existing normal snapshots, fileName = " << fileName;
        }
    }

    // since the file record is not persistent, after mds switching the leader,
    // file record manager is empty
    // after file is opened, there will be refresh session requests in each
    // file record expiration time
    // so wait for a file record expiration time to make sure that
    // the file record is updated
    if (!IsStartEnoughTime(1)) {
        LOG(WARNING) << "MDS doesn't start enough time";
        return StatusCode::kNotSupported;
    }

    std::vector<butil::EndPoint> endPoints;
    if (fileRecordManager_->FindFileMountPoint(fileName, &endPoints) &&
        !endPoints.empty()) {
        LOG(WARNING) << fileName << " has " << endPoints.size()
                     << " mount points";
        return StatusCode::kFileOccupied;
    }

    return StatusCode::kOK;
}

/**
 * File is not allowed to rollback when there are clients mounting it
 * or abnormal snapshot
*/
StatusCode CurveFS::CheckFileCanRecover(const std::string &fileName, FileStatus snapStatus) {
    if (snapStatus == FileStatus::kFileDeleting) {
        LOG(INFO) << "fileName = " << fileName << ", snapshot is under deleting";
        return StatusCode::kSnapshotDeleting;
    }
    if (snapStatus == FileStatus::kFileToBeDeleted) {
        LOG(INFO) << "fileName = " << fileName << ", snapshot is marked to be deleted";
        return StatusCode::kSnapshotToBeDeleted;
    }
    if (snapStatus != FileStatus::kFileCreated) {
        LOG(ERROR) << "fileName = " << fileName 
                   << ", status error, status = " << snapStatus;
        return StatusCode::KInternalError;
    }
    // since the file record is not persistent, after mds switching the leader,
    // file record manager is empty
    // after file is opened, there will be refresh session requests in each
    // file record expiration time
    // so wait for a file record expiration time to make sure that
    // the file record is updated
    if (!IsStartEnoughTime(1)) {
        LOG(WARNING) << "MDS doesn't start enough time";
        return StatusCode::kNotSupported;
    }

    std::vector<butil::EndPoint> endPoints;
    if (fileRecordManager_->FindFileMountPoint(fileName, &endPoints) &&
        !endPoints.empty()) {
        LOG(WARNING) << fileName << " has " << endPoints.size()
                     << " mount points";
        return StatusCode::kFileOccupied;
    }

    return StatusCode::kOK;
}

StatusCode CurveFS::CheckSnapshotCanDelete(const std::string &fileName, FileStatus snapStatus) {
   if (snapStatus == FileStatus::kFileToBeDeleted) {
        LOG(INFO) << "fileName = " << fileName
                  << ", snapshot was already marked to be deleted";
        return StatusCode::kSnapshotToBeDeleted;
    }

    if (snapStatus == FileStatus::kFileDeleting) {
        LOG(INFO) << "fileName = " << fileName
                  << ", snapshot is under deleting";
        return StatusCode::kSnapshotDeleting;
    }

    if (snapStatus != FileStatus::kFileCreated) {
        LOG(ERROR) << "fileName = " << fileName
                   << ", status error, status = " << snapStatus;
        return StatusCode::KInternalError;
    }
    return StatusCode::kOK;
}

// TODO(hzchenwei3): change oldFileName to sourceFileName
//                   and newFileName to destFileName)
StatusCode CurveFS::RenameFile(const std::string & oldFileName,
                               const std::string & newFileName,
                               uint64_t oldFileId, uint64_t newFileId) {
    if (oldFileName == "/" || newFileName  == "/") {
        return StatusCode::kParaError;
    }

    if (!oldFileName.compare(newFileName)) {
        LOG(INFO) << "rename same name, oldFileName = " << oldFileName
                  << ", newFileName = " << newFileName;
        return StatusCode::kFileExists;
    }

    FileInfo  oldFileInfo;
    StatusCode ret = GetFileInfo(oldFileName, &oldFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(INFO) << "get source file error, errCode = " << ret;
        return ret;
    }

    if (oldFileId != kUnitializedFileID && oldFileId != oldFileInfo.id()) {
        LOG(WARNING) << "rename file, oldFileId missmatch"
                   << ", oldFileName = " << oldFileName
                   << ", newFileName = " << newFileName
                   << ", oldFileInfo.id() = " << oldFileInfo.id()
                   << ", oldFileId = " << oldFileId;
        return StatusCode::kFileIdNotMatch;
    }

    // only the rename of INODE_PAGEFILE is supported
    if (oldFileInfo.filetype() != FileType::INODE_PAGEFILE) {
        LOG(ERROR) << "rename oldFileName = " << oldFileName
                   << ", fileType not support, fileType = "
                   << oldFileInfo.filetype();
        return StatusCode::kNotSupported;
    }

    // determine whether oldFileName can be renamed (whether being used,
    // during snapshot or being cloned)
    ret = CheckFileCanChange(oldFileName, oldFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "rename fail, can not rename file"
                << ", oldFileName = " << oldFileName
                << ", ret = " << ret;
        return ret;
    }

    FileInfo parentFileInfo;
    std::string  lastEntry;
    auto ret2 = WalkPath(newFileName, &parentFileInfo, &lastEntry);
    if (ret2 != StatusCode::kOK) {
        LOG(WARNING) << "dest middle dir not exist";
        return StatusCode::kFileNotExists;
    }

    FileInfo existNewFileInfo;
    auto ret3 = LookUpFile(parentFileInfo, lastEntry, &existNewFileInfo);
    if (ret3 == StatusCode::kOK) {
        if (newFileId != kUnitializedFileID
            && newFileId != existNewFileInfo.id()) {
            LOG(WARNING) << "rename file, newFileId missmatch"
                        << ", oldFileName = " << oldFileName
                        << ", newFileName = " << newFileName
                        << ", newFileInfo.id() = " << existNewFileInfo.id()
                        << ", newFileId = " << newFileId;
            return StatusCode::kFileIdNotMatch;
        }

        // determine whether it can be covered. Judge the file type first
         if (existNewFileInfo.filetype() != FileType::INODE_PAGEFILE) {
            LOG(ERROR) << "rename oldFileName = " << oldFileName
                       << " to newFileName = " << newFileName
                       << "file type mismatch. old fileType = "
                       << oldFileInfo.filetype() << ", new fileType = "
                       << existNewFileInfo.filetype();
            return StatusCode::kFileExists;
        }

        // determine whether newFileName can be renamed (whether being used,
        // during snapshot or being cloned)
        StatusCode ret = CheckFileCanChange(newFileName, existNewFileInfo);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "cannot rename file"
                        << ", newFileName = " << newFileName
                        << ", ret = " << ret;
            return ret;
        }

        // move existNewFileInfo to the recycle bin
        FileInfo recycleFileInfo;
        recycleFileInfo.CopyFrom(existNewFileInfo);
        recycleFileInfo.set_parentid(RECYCLEBININODEID);
        recycleFileInfo.set_filename(recycleFileInfo.filename() + "-" +
                std::to_string(recycleFileInfo.id()));
        recycleFileInfo.set_originalfullpathname(newFileName);

        // rename!
        FileInfo newFileInfo;
        newFileInfo.CopyFrom(oldFileInfo);
        newFileInfo.set_parentid(parentFileInfo.id());
        newFileInfo.set_filename(lastEntry);

        auto ret1 = storage_->ReplaceFileAndRecycleOldFile(oldFileInfo,
                                                        newFileInfo,
                                                        existNewFileInfo,
                                                        recycleFileInfo);
        if (ret1 != StoreStatus::OK) {
            LOG(ERROR) << "storage_ ReplaceFileAndRecycleOldFile error"
                        << ", oldFileName = " << oldFileName
                        << ", newFileName = " << newFileName
                        << ", ret = " << ret1;

            return StatusCode::kStorageError;
        }
        return StatusCode::kOK;
    } else if (ret3 == StatusCode::kFileNotExists) {
        // newFileName does not exist, rename directly
        FileInfo newFileInfo;
        newFileInfo.CopyFrom(oldFileInfo);
        newFileInfo.set_parentid(parentFileInfo.id());
        newFileInfo.set_filename(lastEntry);

        auto ret = storage_->RenameFile(oldFileInfo, newFileInfo);
        if ( ret != StoreStatus::OK ) {
            LOG(ERROR) << "storage_ renamefile error, error = " << ret;
            return StatusCode::kStorageError;
        }
        return StatusCode::kOK;
    } else {
        LOG(INFO) << "dest file LookUpFile return: " << ret3;
        return ret3;
    }
}

StatusCode CurveFS::ExtendFile(const std::string &filename,
                               uint64_t newLength) {
    FileInfo  fileInfo;
    auto ret = GetFileInfo(filename, &fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(INFO) << "get source file error, errCode = " << ret;
        return  ret;
    }

    if (fileInfo.filetype() != FileType::INODE_PAGEFILE) {
        LOG(INFO) << "Only INODE_PAGEFILE support extent";
        return StatusCode::kNotSupported;
    }

    if (newLength > maxFileLength_) {
        LOG(ERROR) << "ExtendFile newLength > maxFileLength, fileName = "
                       << filename << ", newLength = " << newLength
                       << ", maxFileLength = " << maxFileLength_;
            return StatusCode::kFileLengthNotSupported;
    }

    if (newLength < fileInfo.length()) {
        LOG(INFO) << "newLength = " << newLength
                  << ", smaller than file length " << fileInfo.length();
        return StatusCode::kShrinkBiggerFile;
    } else if (newLength == fileInfo.length()) {
        LOG(INFO) << "newLength equals file length" << newLength;
        return StatusCode::kOK;
    } else {
        uint64_t deltaLength = newLength - fileInfo.length();
        if (fileInfo.segmentsize() == 0) {
            LOG(ERROR) << "segmentsize = 0 , filename = " << filename;
            return StatusCode::KInternalError;
        }
        if (deltaLength % fileInfo.segmentsize()  != 0) {
            LOG(INFO) << "extent unit error, mini extentsize = "
                      << fileInfo.segmentsize();
            return   StatusCode::kExtentUnitError;
        }
        fileInfo.set_length(newLength);
        return PutFile(fileInfo);
    }
}

StatusCode CurveFS::ChangeOwner(const std::string &filename,
                                const std::string &newOwner) {
    FileInfo  fileInfo;
    StatusCode ret = GetFileInfo(filename, &fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(INFO) << "get source file error, errCode = " << ret;
        return  ret;
    }

    if (newOwner == fileInfo.owner()) {
        LOG(INFO) << "change owner sucess, file owner is same with newOwner, "
                  << "filename = " << filename
                  << ", file.owner() = " << fileInfo.owner()
                  << ", newOwner = " << newOwner;
        return StatusCode::kOK;
    }

    // check whether change owner is supported. Only INODE_PAGEFILE
    // and INODE_DIRECTORY are supported
    if (fileInfo.filetype() == FileType::INODE_PAGEFILE) {
        // determine whether the owner of the file can be changed (whether
        // the file is being used, during snapshot or being cloned)
        ret = CheckFileCanChange(filename, fileInfo);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "cannot changeOwner file"
                        << ", filename = " << filename
                        << ", ret = " << ret;
            return ret;
        }
    } else if (fileInfo.filetype() == FileType::INODE_DIRECTORY) {
        // if there are files in the directory, can not change owner
        bool isEmpty = false;
        ret = isDirectoryEmpty(fileInfo, &isEmpty);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "check is directory empty fail, filename = "
                       << filename << ", ret = " << ret;
            return ret;
        }
        if (!isEmpty) {
            LOG(WARNING) << "ChangeOwner fail, file is directory and not empty"
                       << ", filename = " << filename;
            return StatusCode::kDirNotEmpty;
        }
    } else {
        LOG(ERROR) << "file type not support change owner"
                  << ", filename = " << filename;
        return StatusCode::kNotSupported;
    }

    // change owner!
    fileInfo.set_owner(newOwner);
    return PutFile(fileInfo);
}

StatusCode CurveFS::GetOrAllocateSegment(const std::string & filename,
        offset_t offset, bool allocateIfNoExist,
        PageFileSegment *segment) {
    assert(segment != nullptr);

    FileInfo  fileInfo;
    auto ret = GetFileInfo(filename, &fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(INFO) << "get source file error, errCode = " << ret;
        return  ret;
    }

    if (fileInfo.filetype() != FileType::INODE_PAGEFILE) {
        LOG(INFO) << "not pageFile, can't do this";
        return StatusCode::kParaError;
    }

    if (offset % fileInfo.segmentsize() != 0) {
        LOG(INFO) << "offset not align with segment";
        return StatusCode::kParaError;
    }

    if (offset + fileInfo.segmentsize() > fileInfo.length()) {
        LOG(INFO) << "bigger than file length, first extentFile";
        return StatusCode::kParaError;
    }

    auto storeRet = storage_->GetSegment(fileInfo.id(), offset, segment);
    if (storeRet == StoreStatus::OK) {
        return StatusCode::kOK;
    } else if (storeRet == StoreStatus::KeyNotExist) {
        if (allocateIfNoExist == false) {
            LOG(INFO) << "file = " << filename <<", segment offset = " << offset
                      << ", not allocated";
            return  StatusCode::kSegmentNotAllocated;
        } else {
            // TODO(hzsunjianliang): check the user and define the logical pool
            auto ifok = chunkSegAllocator_->AllocateChunkSegment(
                            fileInfo.filetype(), fileInfo.segmentsize(),
                            fileInfo.chunksize(), offset, segment);
            if (ifok == false) {
                LOG(ERROR) << "AllocateChunkSegment error";
                return StatusCode::kSegmentAllocateError;
            }
            int64_t revision;
            if (storage_->PutSegment(fileInfo.id(), offset, segment, &revision)
                != StoreStatus::OK) {
                LOG(ERROR) << "PutSegment fail, fileInfo.id() = "
                           << fileInfo.id()
                           << ", offset = "
                           << offset;
                return StatusCode::kStorageError;
            }
            allocStatistic_->AllocSpace(segment->logicalpoolid(),
                    segment->segmentsize(),
                    revision);

            LOG(INFO) << "alloc segment success, fileInfo.id() = "
                      << fileInfo.id()
                      << ", offset = " << offset;
            return StatusCode::kOK;
        }
    }  else {
        return StatusCode::KInternalError;
    }
}

StatusCode CurveFS::CreateSnapShotFile(const std::string &fileName,
                                    FileInfo *snapshotFileInfo) {
    FileInfo  parentFileInfo;
    std::string lastEntry;

    auto ret = WalkPath(fileName, &parentFileInfo, &lastEntry);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << fileName << ", Walk Path error";
        return ret;
    }
    if (lastEntry.empty()) {
        return StatusCode::kFileNotExists;
    }

    FileInfo fileInfo;
    ret = LookUpFile(parentFileInfo, lastEntry, &fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << fileName << ", LookUpFile error";
        return ret;
    }

    // check file type
    if (fileInfo.filetype() != FileType::INODE_PAGEFILE) {
        return StatusCode::kNotSupported;
    }

    // compatibility check, when the client version does not exist or is lower
    // than 0.0.6, snapshots are not allowed
    ret = IsSnapshotAllowed(fileName);
    if (ret != kOK) {
        return ret;
    }

    // check whether snapshot exist
    std::vector<FileInfo> snapShotFiles;
    if (storage_->ListSnapshotFile(fileInfo.id(),
                  fileInfo.id() + 1, &snapShotFiles) != StoreStatus::OK ) {
        LOG(ERROR) << fileName  << "listFile fail";
        return StatusCode::kStorageError;
    }
    if (snapShotFiles.size() != 0) {
        LOG(INFO) << fileName << " exist snapshotfile, num = "
            << snapShotFiles.size()
            << ", snapShotFiles[0].seqNum = " << snapShotFiles[0].seqnum();
        *snapshotFileInfo = snapShotFiles[0];
        return StatusCode::kFileUnderSnapShot;
    }

    // TTODO(hzsunjianliang): check if fileis open and session not expire
    // then invalide client

    // do snapshot
    // first new snapshot fileinfo, based on the original fileInfo
    InodeID inodeID;
    if (InodeIDGenerator_->GenInodeID(&inodeID) != true) {
        LOG(ERROR) << fileName << ", createSnapShotFile GenInodeID error";
        return StatusCode::kStorageError;
    }
    *snapshotFileInfo = fileInfo;
    snapshotFileInfo->set_filetype(FileType::INODE_SNAPSHOT_PAGEFILE);
    snapshotFileInfo->set_id(inodeID);
    snapshotFileInfo->set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());
    snapshotFileInfo->set_parentid(fileInfo.id());
    snapshotFileInfo->set_filename(fileInfo.filename() + "-" +
            std::to_string(fileInfo.seqnum()));
    snapshotFileInfo->set_filestatus(FileStatus::kFileCreated);

    // add original file snapshot seq number
    fileInfo.set_seqnum(fileInfo.seqnum() + 1);

    // do storage
    ret = SnapShotFile(&fileInfo, snapshotFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << fileName << ", SnapShotFile error";
        return StatusCode::kStorageError;
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::CreateSnapShotFile2(const std::string &fileName,
                                    FileInfo *snapshotFileInfo) {
    FileInfo  parentFileInfo;
    std::string lastEntry;

    auto ret = WalkPath(fileName, &parentFileInfo, &lastEntry);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << fileName << ", Walk Path error";
        return ret;
    }
    if (lastEntry.empty()) {
        return StatusCode::kFileNotExists;
    }

    FileInfo fileInfo;
    ret = LookUpFile(parentFileInfo, lastEntry, &fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << fileName << ", LookUpFile error";
        return ret;
    }

    // check file type
    if (fileInfo.filetype() != FileType::INODE_PAGEFILE) {
        return StatusCode::kNotSupported;
    }

    // compatibility check, when the client version does not exist or is lower
    // than 0.0.6, snapshots are not allowed
    ret = IsSnapshotAllowed(fileName);
    if (ret != kOK) {
        return ret;
    }

    // Not allowed to create snapshot when file is under deleting
    if (fileInfo.filestatus() == FileStatus::kFileDeleting) {
        LOG(INFO) << "fileName = " << fileName << " is under deleting";
        return StatusCode::kFileUnderDeleting;
    }

    // TTODO(hzsunjianliang): check if fileis open and session not expire
    // then invalide client

    // do snapshot
    // first new snapshot fileinfo, based on the original fileInfo
    InodeID inodeID;
    if (InodeIDGenerator_->GenInodeID(&inodeID) != true) {
        LOG(ERROR) << fileName << ", createSnapShotFile GenInodeID error";
        return StatusCode::kStorageError;
    }
    *snapshotFileInfo = fileInfo;
    snapshotFileInfo->set_filetype(FileType::INODE_SNAPSHOT_PAGEFILE);
    snapshotFileInfo->set_id(inodeID);
    snapshotFileInfo->set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());
    snapshotFileInfo->set_parentid(fileInfo.id());
    snapshotFileInfo->set_filename(fileInfo.filename() + "-" +
            std::to_string(fileInfo.seqnum()));
    snapshotFileInfo->set_filestatus(FileStatus::kFileCreated);

    // save snapshot seq number to latest clones in fileInfo,
    // in order to let client know the latest snaps through RefreshSession rpc
    int i = fileInfo.clones_size() - 1;
    for (; i >= 0; i--) {
        if (fileInfo.clones(i).seqnum() == fileInfo.seqnum()) {
            fileInfo.mutable_clones(i)->add_snaps(fileInfo.seqnum());
            fileInfo.mutable_clones(i)->set_seqnum(fileInfo.seqnum() + 1);
            break;
        }
    }
    if (i < 0) {
        LOG(ERROR) << "Found invalid fileinfo when create snapshot, fileInfo = \n"
                   << fileInfo.DebugString();
        return StatusCode::kParaError;
    }
    // add original file snapshot seq number
    fileInfo.set_seqnum(fileInfo.seqnum() + 1);

    // do storage
    ret = SnapShotFile(&fileInfo, snapshotFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << fileName << ", SnapShotFile error";
        return StatusCode::kStorageError;
    }
    LOG(INFO) << "CreateSnapShotFile2 update file to storage success"
              << ", fileInfo: \n" << fileInfo.DebugString();  
    return StatusCode::kOK;
}

StatusCode CurveFS::ListSnapShotFile(const std::string & fileName,
                            std::vector<FileInfo> *snapshotFileInfos, FileInfo *srcfileInfo) const {
    FileInfo parentFileInfo;
    std::string lastEntry;

    auto ret = WalkPath(fileName, &parentFileInfo, &lastEntry);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << fileName << ", Walk Path error";
        return ret;
    }
    if (lastEntry.empty()) {
        LOG(INFO) << fileName << ", dir not suppport SnapShot";
        return StatusCode::kNotSupported;
    }

    FileInfo tmpfileInfo;
    FileInfo* fileInfo = srcfileInfo != nullptr ? srcfileInfo : &tmpfileInfo;
    
    ret = LookUpFile(parentFileInfo, lastEntry, fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << fileName << ", LookUpFile error";
        return ret;
    }

    // check file type
    if (fileInfo->filetype() != FileType::INODE_PAGEFILE) {
        LOG(INFO) << fileName << ", filetype not support SnapShot";
        return StatusCode::kNotSupported;
    }

    // list snapshot files
    auto storeStatus =  storage_->ListSnapshotFile(fileInfo->id(),
                                           fileInfo->id() + 1,
                                           snapshotFileInfos);
    if (storeStatus == StoreStatus::KeyNotExist ||
        storeStatus == StoreStatus::OK) {
        return StatusCode::kOK;
    } else {
        LOG(ERROR) << fileName << ", storage ListFile return = " << storeStatus;
        return StatusCode::kStorageError;
    }
}

StatusCode CurveFS::GetSnapShotFileInfo(const std::string &fileName,
                        FileSeqType seq, FileInfo *snapshotFileInfo, FileInfo *fileInfo) const {
    std::vector<FileInfo> snapShotFileInfos;
    StatusCode ret =  ListSnapShotFile(fileName, &snapShotFileInfos, fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(INFO) << "ListSnapShotFile error";
        return ret;
    }

    if (snapShotFileInfos.size() == 0) {
        LOG(INFO) << "file not under snapshot";
        return StatusCode::kSnapshotFileNotExists;
    }

    unsigned int index;
    for ( index = 0; index != snapShotFileInfos.size(); index++ ) {
        if (snapShotFileInfos[index].seqnum() == static_cast<uint64_t>(seq)) {
          break;
        }
    }
    if (index == snapShotFileInfos.size()) {
        LOG(INFO) << fileName << " snapshotFile seq = " << seq << " not find";
        return StatusCode::kSnapshotFileNotExists;
    }

    *snapshotFileInfo = snapShotFileInfos[index];
    return StatusCode::kOK;
}

StatusCode CurveFS::DeleteFileSnapShotFile(const std::string &fileName,
                        FileSeqType seq,
                        std::shared_ptr<AsyncDeleteSnapShotEntity> entity) {
    FileInfo snapShotFileInfo;
    StatusCode ret =  GetSnapShotFileInfo(fileName, seq, &snapShotFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(INFO) << "fileName = " << fileName
            << ", seq = "<< seq
            << ", GetSnapShotFileInfo file ,ret = " << ret;
        return ret;
    } else {
        LOG(INFO) << "snapshotfile info = " << snapShotFileInfo.filename();
    }

    if (snapShotFileInfo.filestatus() == FileStatus::kFileDeleting) {
        LOG(INFO) << "fileName = " << fileName
        << ", seq = " << seq
        << ", snapshot is under deleting";
        return StatusCode::kSnapshotDeleting;
    }

    if (snapShotFileInfo.filestatus() != FileStatus::kFileCreated) {
        LOG(ERROR) << "fileName = " << fileName
        << ", seq = " << seq
        << ", status error, status = " << snapShotFileInfo.filestatus();
        return StatusCode::KInternalError;
    }

    snapShotFileInfo.set_filestatus(FileStatus::kFileDeleting);
    ret = PutFile(snapShotFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "fileName = " << fileName
            << ", seq = " << seq
            << ", PutFile error = " << ret;
        return StatusCode::KInternalError;
    }

    //  message the snapshot delete manager
    if (!cleanManager_->SubmitDeleteSnapShotFileJob(
                        snapShotFileInfo, entity)) {
        LOG(ERROR) << "fileName = " << fileName
                << ", seq = " << seq
                << ", Delete Task Deduplicated";
        return StatusCode::KInternalError;
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::DeleteFileSnapShotFile2(const std::string &fileName,
                        FileSeqType seq,
                        std::shared_ptr<AsyncDeleteSnapShotEntity> entity) {
    return DeleteSnapshot(fileName, seq, false);
}

StatusCode CurveFS::RecoverFile2Snap(const std::string &fileName, FileSeqType seq) {
    FileInfo snapShotFileInfo, fileInfo;
    StatusCode ret =  GetSnapShotFileInfo(fileName, seq, &snapShotFileInfo, &fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(INFO) << "fileName = " << fileName << ", seq = "<< seq
                  << ", GetSnapShotFileInfo file, ret = " << ret;
        return ret;
    }
    // Not allowed to recover when file is under deleting
    if (fileInfo.filestatus() == FileStatus::kFileDeleting) {
        LOG(INFO) << "fileName = " << fileName << " is under deleting";
        return StatusCode::kFileUnderDeleting;
    }

    ret = CheckFileCanRecover(fileName, snapShotFileInfo.filestatus());
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "cannot recover file " << fileName 
                   << ", seq = " << seq << ", ret = " << ret;
        return ret;
    }

    InodeID inodeID;
    if (InodeIDGenerator_->GenInodeID(&inodeID) != true) {
        LOG(ERROR) << fileName << ", RecoverFile2Snap GenInodeID error";
        return StatusCode::kStorageError;
    }
    // this presents the latest file before recovering, aka inner snapshot
    FileInfo hiddenFileInfo;
    hiddenFileInfo = fileInfo;
    hiddenFileInfo.set_filetype(FileType::INODE_SNAPSHOT_PAGEFILE);
    hiddenFileInfo.set_id(inodeID);
    hiddenFileInfo.set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());
    hiddenFileInfo.set_parentid(fileInfo.id());
    // distinguish inner snapshot by "inner" contained in the name
    hiddenFileInfo.set_filename(fileInfo.filename() + "-" +
            std::to_string(fileInfo.seqnum()) + "-inner");
    auto curID = fileInfo.clones().rbegin()->id();
    // If the old clonefileinfo has no snapshots, it can be deleted.
    // Otherwise, it's marked as to be deleted and the deletion process starts
    // once all of its snapshots have been deleted.
    if (hiddenFileInfo.clones().rbegin()->snaps_size() == 0) {
        hiddenFileInfo.set_filestatus(FileStatus::kFileDeleting);
        fileInfo.mutable_clones()->RemoveLast();
    } else {
        hiddenFileInfo.set_filestatus(FileStatus::kFileToBeDeleted);
    }

    // When recovering file to a snapshot, we need to create a new clonefileinfo 
    // and increment seqnum of file.
    auto oldSeqNum = fileInfo.seqnum();
    auto clone = fileInfo.add_clones();
    clone->set_id(curID + 1);
    clone->set_seqnum(oldSeqNum + 1);
    clone->set_recoversource(seq);
    fileInfo.set_seqnum(oldSeqNum + 1);
    // set file length to snapshot length considering file extent scenario
    fileInfo.set_length(snapShotFileInfo.length());

    ret = SnapShotFile(&fileInfo, &hiddenFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "fileName = " << fileName << ", recover to seq = " << seq
                   << ", SnapShotFile error = " << ret;
        return StatusCode::kStorageError;
    }
    LOG(INFO) << "RecoverFile2Snap update file to storage success"
              << ", fileInfo: \n" << fileInfo.DebugString()
              << ", hiddenFileInfo: \n" << hiddenFileInfo.DebugString();

    if (hiddenFileInfo.filestatus() == FileStatus::kFileDeleting) {
        if (!cleanManager_->SubmitDeleteBatchSnapShotFileJob(
                            hiddenFileInfo, nullptr)) {
            LOG(ERROR) << "fileName = " << fileName
                       << ", seq = " << seq
                       << ", Delete Task Deduplicated";
            return StatusCode::KInternalError;
        }
        // when a clone file is deleted, the recover source snapshot may have
        // no dependency and can be deleted.
        auto recoversource = hiddenFileInfo.clones().rbegin()->recoversource();
        ret = DeleteSnapshot(fileName, recoversource, true);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "fileName = " << fileName << ", recoversource = " << recoversource
                       << ", DeleteSnapshot when recover, error = " << ret;
            return StatusCode::kStorageError;
        }

    }
    return StatusCode::kOK;
}

StatusCode CurveFS::DeleteSnapshot(const std::string &fileName, FileSeqType seq, bool isCheck) {
    FileInfo snapShotFileInfo, fileInfo;
    StatusCode ret =  GetSnapShotFileInfo(fileName, seq, &snapShotFileInfo, &fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(INFO) << "fileName = " << fileName << ", seq = "<< seq
                  << ", GetSnapShotFileInfo file, ret = " << ret;
        return ret;
    }
    
    if (isCheck) {
        // Only when the snapshot was marked to be deleted and not referenced by any
        // rollback file (i.e. not the recoversource of any clone file), it can be deleted.
        if (snapShotFileInfo.filestatus() != FileStatus::kFileToBeDeleted) {
            return StatusCode::kOK;
        }
        for (auto& clone:fileInfo.clones()) {
            if (clone.recoversource() == seq) {
                return StatusCode::kOK;
            }
        }
    } else {
        ret = CheckSnapshotCanDelete(fileName, snapShotFileInfo.filestatus());
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "cannot delete snapshot " << fileName 
                    << ", seq = " << seq << ", ret = " << ret;
            return ret;
        }
        // Only mark the snapshot as to be deleted . The deletion process starts once this
        // snapshot is not the recoversource of any file (i.e. not referenced by rollback).
        for (auto& clone:fileInfo.clones()) {
            if (clone.recoversource() == seq) {
                snapShotFileInfo.set_filestatus(FileStatus::kFileToBeDeleted);
                ret = PutFile(snapShotFileInfo);
                if (ret != StatusCode::kOK) {
                    LOG(ERROR) << "fileName = " << fileName << ", seq = " << seq
                            << ", PutFile error = " << ret;
                    return StatusCode::kStorageError;
                }
                LOG(INFO) << "DeleteSnapshot fileName = " << fileName 
                        << ", mark snap sn " << seq << " as to be deleted success" 
                        << ", fileInfo = \n" << fileInfo.DebugString();
                return StatusCode::kOK;
            }
        }
    }
    // the snapshot is not referenced by any rollbacked file, start the deletion job
    return DeleteSnapshotInner(snapShotFileInfo, fileInfo);
}

StatusCode CurveFS::DeleteSnapshotInner(FileInfo &snapShotFileInfo, FileInfo &fileInfo) {
    FileInfo hiddenFileInfo; // for rollback that may be deleted
    bool deleteCloneWhenEmptySnaps = false;
    auto seq = snapShotFileInfo.seqnum();
    std::string fileName = fileInfo.filename();
    snapShotFileInfo.set_filestatus(FileStatus::kFileDeleting);
    auto cloneIter = fileInfo.mutable_clones()->begin();
    for (; cloneIter != fileInfo.mutable_clones()->end(); ++cloneIter) {
        if (seq < cloneIter->seqnum()) {
            auto snaps = cloneIter->mutable_snaps();
            auto findSnapIter = std::find(snaps->begin(), snaps->end(), seq);
            if (findSnapIter == snaps->end()) {
                LOG(WARNING) << "fileName = " << fileName << ", seq = " << seq 
                             << ", not found in fileInfo!";
            } else {
                snaps->erase(findSnapIter);
            }
            // if this clone file has empty snaps and not the latest clone file, which
            // also means it has been marked as to be deleted, it's time to delete it. 
            if (snaps->empty() && cloneIter != fileInfo.mutable_clones()->end() - 1) {
                // create an inner snapshot presenting this clone file soon to be deleted
                InodeID inodeID;
                if (InodeIDGenerator_->GenInodeID(&inodeID) != true) {
                    LOG(ERROR) << fileName << ", DeleteFileSnapShotFile2 GenInodeID error";
                    return StatusCode::kStorageError;
                }
                deleteCloneWhenEmptySnaps = true;
                hiddenFileInfo = fileInfo;
                hiddenFileInfo.set_filetype(FileType::INODE_SNAPSHOT_PAGEFILE);
                hiddenFileInfo.set_id(inodeID);
                hiddenFileInfo.set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());
                hiddenFileInfo.set_parentid(fileInfo.id());
                // distinguish inner snapshot by "inner" contained in the name
                hiddenFileInfo.set_filename(fileInfo.filename() + "-" +
                        std::to_string(cloneIter->seqnum()) + "-inner");
                hiddenFileInfo.set_seqnum(cloneIter->seqnum());
                hiddenFileInfo.clear_clones();
                auto fakeClone = hiddenFileInfo.add_clones();
                fakeClone->CopyFrom(*cloneIter);
                hiddenFileInfo.set_filestatus(FileStatus::kFileDeleting);
                // update fileInfo
                fileInfo.mutable_clones()->erase(cloneIter);
            }

            break;
        }        
    }
    StatusCode ret;
    if (deleteCloneWhenEmptySnaps) {
        ret = SnapShotFile(&fileInfo, &snapShotFileInfo, &hiddenFileInfo);
    } else {
        ret = SnapShotFile(&fileInfo, &snapShotFileInfo);
    }
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "fileName = " << fileName << ", seq = " << seq
                   << ", SnapShotFile error = " << ret;
        return StatusCode::kStorageError;
    }
    LOG(INFO) << "DoDeleteSnapshot update snap and file to storage success"
              << ", snapInfo: \n" << snapShotFileInfo.DebugString()
              << ", fileInfo: \n" << fileInfo.DebugString();
    LOG_IF(INFO, deleteCloneWhenEmptySnaps)
           << "DoDeleteSnapshot update deleting clone file to storage success"
           << ", hiddenFileInfo: \n" << hiddenFileInfo.DebugString();

    if (!cleanManager_->SubmitDeleteBatchSnapShotFileJob(
                        snapShotFileInfo, nullptr)) {
        LOG(ERROR) << "fileName = " << fileName
                   << ", seq = " << seq
                   << ", Delete Task Deduplicated";
        return StatusCode::KInternalError;
    }
    if (deleteCloneWhenEmptySnaps) {
        if (!cleanManager_->SubmitDeleteBatchSnapShotFileJob(
                            hiddenFileInfo, nullptr)) {
            LOG(ERROR) << "fileName = " << fileName
                       << ", seq = " << seq
                       << ", Delete Task Deduplicated";
            return StatusCode::KInternalError;
        } 
        // when a clone file is deleted, the recover source snapshot may have
        // no dependency and can be deleted.
        auto recoversource = hiddenFileInfo.clones().rbegin()->recoversource();
        ret = DeleteSnapshot(fileName, recoversource, true);
        if (ret != StatusCode::kOK) {
            LOG(ERROR) << "fileName = " << fileName << ", seq = " << recoversource
                       << ", recursion DeleteSnapshot error = " << ret;
            return ret;
        }
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::CheckSnapShotFileStatus(const std::string &fileName,
                                    FileSeqType seq, FileStatus * status,
                                    uint32_t * progress) const {
    FileInfo snapShotFileInfo;
    StatusCode ret =  GetSnapShotFileInfo(fileName, seq, &snapShotFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(WARNING) << "GetSnapShotFileInfo file fail, fileName = "
                   << fileName << ", seq = " << seq << ", ret = " << ret;
        return ret;
    }

    *status = snapShotFileInfo.filestatus();
    if (snapShotFileInfo.filestatus() == FileStatus::kFileDeleting) {
        TaskIDType taskID = static_cast<TaskIDType>(snapShotFileInfo.id());
        auto task = cleanManager_->GetTask(taskID);
        if (task == nullptr) {
            // GetSnapShotFileInfo again
            StatusCode ret2 =
                GetSnapShotFileInfo(fileName, seq, &snapShotFileInfo);
            // if not exist, means delete succeed.
            if (StatusCode::kSnapshotFileNotExists == ret2) {
                *progress = 100;
                return StatusCode::kSnapshotFileNotExists;
            // else the snapshotFile still exist,
            // means delete failed and retry times exceed.
            } else {
                *progress = 0;
                LOG(ERROR) << "snapshot file delete fail, fileName = "
                           << fileName << ", seq = " << seq;
                return StatusCode::kSnapshotFileDeleteError;
            }
        }

        TaskStatus taskStatus = task->GetTaskProgress().GetStatus();
        switch (taskStatus) {
            case TaskStatus::PROGRESSING:
            case TaskStatus::FAILED:  // FAILED task will retry
                *progress = task->GetTaskProgress().GetProgress();
                break;
            case TaskStatus::SUCCESS:
                *progress = 100;
                break;
        }
    } else {
        // means delete haven't begin.
        *progress = 0;
    }

    return StatusCode::kOK;
}

StatusCode CurveFS::CheckSnapShotFileStatus2(const std::string &fileName,
                                    FileSeqType seq, FileStatus * status,
                                    uint32_t * progress) const {
    FileInfo snapShotFileInfo;
    StatusCode ret =  GetSnapShotFileInfo(fileName, seq, &snapShotFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(WARNING) << "GetSnapShotFileInfo file fail, fileName = "
                   << fileName << ", seq = " << seq << ", ret = " << ret;
        return ret;
    }

    *status = snapShotFileInfo.filestatus();
    if (snapShotFileInfo.filestatus() == FileStatus::kFileDeleting) {
        TaskIDType fileID = static_cast<TaskIDType>(snapShotFileInfo.parentid());
        TaskIDType snapSn = static_cast<TaskIDType>(snapShotFileInfo.seqnum());
        auto task = cleanManager_->GetTask(fileID, snapSn);
        if (task == nullptr) {
            // GetSnapShotFileInfo again
            StatusCode ret2 =
                GetSnapShotFileInfo(fileName, seq, &snapShotFileInfo);
            // if not exist, means delete succeed.
            if (StatusCode::kSnapshotFileNotExists == ret2) {
                *progress = 100;
                return StatusCode::kSnapshotFileNotExists;
            // else the snapshotFile still exist,
            // means delete failed and retry times exceed.
            } else {
                *progress = 0;
                LOG(ERROR) << "snapshot file delete fail, fileName = "
                           << fileName << ", seq = " << seq;
                return StatusCode::kSnapshotFileDeleteError;
            }
        }

        TaskStatus taskStatus = task->GetTaskProgress().GetStatus();
        switch (taskStatus) {
            case TaskStatus::PROGRESSING:
            case TaskStatus::FAILED:  // FAILED task will retry
                *progress = task->GetTaskProgress().GetProgress();
                break;
            case TaskStatus::SUCCESS:
                *progress = 100;
                break;
        }
    } else {
        // means delete haven't begin.
        *progress = 0;
    }

    return StatusCode::kOK;
}

StatusCode CurveFS::GetSnapShotFileSegment(
        const std::string & fileName,
        FileSeqType seq,
        offset_t offset,
        PageFileSegment *segment) {
    assert(segment != nullptr);

    FileInfo snapShotFileInfo;
    StatusCode ret = GetSnapShotFileInfo(fileName, seq, &snapShotFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(WARNING) << "GetSnapShotFileInfo file fail, fileName = "
                   << fileName << ", seq = " << seq << ", ret = " << ret;
        return ret;
    }

    if (offset % snapShotFileInfo.segmentsize() != 0) {
        LOG(WARNING) << "offset not align with segment, fileName = "
                   << fileName << ", seq = " << seq;
        return StatusCode::kParaError;
    }

    if (offset + snapShotFileInfo.segmentsize() > snapShotFileInfo.length()) {
        LOG(WARNING) << "bigger than file length, fileName = "
                   << fileName << ", seq = " << seq;
        return StatusCode::kParaError;
    }

    FileInfo fileInfo;
    auto ret1 = GetFileInfo(fileName, &fileInfo);
    if (ret1 != StatusCode::kOK) {
        LOG(ERROR) << "get origin file error, fileName = "
                   << fileName << ", errCode = " << ret1;
        return  ret1;
    }

    if (offset % fileInfo.segmentsize() != 0) {
        LOG(WARNING) << "origin file offset not align with segment, fileName = "
                   << fileName << ", offset = " << offset
                   << ", file segmentsize = " << fileInfo.segmentsize();
        return StatusCode::kParaError;
    }

    if (offset + fileInfo.segmentsize() > fileInfo.length()) {
        LOG(WARNING) << "bigger than origin file length, fileName = "
                   << fileName << ", offset = " << offset
                   << ", file segmentsize = " << fileInfo.segmentsize()
                   << ", file length = " << fileInfo.length();
        return StatusCode::kParaError;
    }

    StoreStatus storeRet = storage_->GetSegment(fileInfo.id(), offset, segment);
    if (storeRet == StoreStatus::OK) {
        return StatusCode::kOK;
    } else if (storeRet == StoreStatus::KeyNotExist) {
        LOG(INFO) << "get segment fail, kSegmentNotAllocated, fileName = "
                  << fileName
                  << ", fileInfo.id() = "
                  << fileInfo.id()
                  << ", offset = " << offset;
        return StatusCode::kSegmentNotAllocated;
    } else {
        LOG(ERROR) << "get segment fail, KInternalError, ret = " << storeRet
                  << ", fileInfo.id() = "
                  << fileInfo.id()
                  << ", offset = " << offset;
        return StatusCode::KInternalError;
    }
}

StatusCode CurveFS::OpenFile(const std::string &fileName,
                             const std::string &clientIP,
                             ProtoSession *protoSession,
                             FileInfo  *fileInfo,
                             CloneSourceSegment* cloneSourceSegment) {
    // check the existence of the file
    StatusCode ret;
    ret = GetFileInfo(fileName, fileInfo);
    if (ret == StatusCode::kFileNotExists) {
        LOG(WARNING) << "OpenFile file not exist, fileName = " << fileName
                   << ", clientIP = " << clientIP
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return ret;
    } else if (ret != StatusCode::kOK) {
        LOG(ERROR) << "OpenFile get file info error, fileName = " << fileName
                   << ", clientIP = " << clientIP
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return ret;
    }

    LOG(INFO) << "FileInfo, " << fileInfo->DebugString();

    if (fileInfo->filetype() != FileType::INODE_PAGEFILE) {
        LOG(ERROR) << "OpenFile file type not support, fileName = " << fileName
                   << ", clientIP = " << clientIP
                   << ", filetype = " << fileInfo->filetype();
        return StatusCode::kNotSupported;
    }

    fileRecordManager_->GetRecordParam(protoSession);

    // clone file
    if (fileInfo->has_clonesource() && isPathValid(fileInfo->clonesource())) {
        return ListCloneSourceFileSegments(fileInfo, cloneSourceSegment);
    }

    return StatusCode::kOK;
}

StatusCode CurveFS::CloseFile(const std::string &fileName,
                              const std::string &sessionID,
                              const std::string &clientIP,
                              uint32_t clientPort) {
    // check the existence of the file
    FileInfo  fileInfo;
    StatusCode ret;
    ret = GetFileInfo(fileName, &fileInfo);
    if (ret == StatusCode::kFileNotExists) {
        LOG(WARNING) << "CloseFile file not exist, fileName = " << fileName
                   << ", sessionID = " << sessionID
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return ret;
    } else if (ret != StatusCode::kOK) {
        LOG(ERROR) << "CloseFile get file info error, fileName = " << fileName
                   << ", sessionID = " << sessionID
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return  ret;
    }

    // remove file record
    fileRecordManager_->RemoveFileRecord(fileName, clientIP, clientPort);

    return StatusCode::kOK;
}

StatusCode CurveFS::RefreshSession(const std::string &fileName,
                            const std::string &sessionid,
                            const uint64_t date,
                            const std::string &signature,
                            const std::string &clientIP,
                            uint32_t clientPort,
                            const std::string &clientVersion,
                            FileInfo  *fileInfo) {
    // check the existence of the file
    StatusCode ret;
    ret = GetFileInfo(fileName, fileInfo);
    if (ret == StatusCode::kFileNotExists) {
        LOG(WARNING) << "RefreshSession file not exist, fileName = " << fileName
                   << fileName
                   << ", sessionid = " << sessionid
                   << ", date = " << date
                   << ", signature = " << signature
                   << ", clientIP = " << clientIP
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return  ret;
    } else if (ret != StatusCode::kOK) {
        LOG(ERROR) << "RefreshSession get file info error, fileName = "
                   << fileName
                   << ", sessionid = " << sessionid
                   << ", date = " << date
                   << ", signature = " << signature
                   << ", clientIP = " << clientIP
                   << ", clientPort = " << clientPort
                   << ", errCode = " << ret
                   << ", errName = " << StatusCode_Name(ret);
        return  ret;
    }

    // update file records
    fileRecordManager_->UpdateFileRecord(fileName, clientVersion, clientIP,
                                         clientPort);

    return StatusCode::kOK;
}

StatusCode CurveFS::CreateCloneFile(const std::string &fileName,
                            const std::string& owner,
                            FileType filetype,
                            uint64_t length,
                            FileSeqType seq,
                            ChunkSizeType chunksize,
                            uint64_t stripeUnit,
                            uint64_t stripeCount,
                            FileInfo *retFileInfo,
                            const std::string & cloneSource,
                            uint64_t cloneLength) {
    // check basic params
    if (filetype != FileType::INODE_PAGEFILE) {
        LOG(WARNING) << "CreateCloneFile err, filename = " << fileName
                << ", filetype not support";
        return StatusCode::kParaError;
    }

    if  (length < minFileLength_ || seq < kStartSeqNum) {
        LOG(WARNING) << "CreateCloneFile err, filename = " << fileName
                    << "file Length < MinFileLength " << minFileLength_
                    << ", length = " << length;
        return StatusCode::kParaError;
    }

    auto ret = CheckStripeParam(stripeUnit, stripeCount);
    if (ret != StatusCode::kOK) {
        return ret;
    }

    // check the existence of the file
    FileInfo parentFileInfo;
    std::string lastEntry;
    ret = WalkPath(fileName, &parentFileInfo, &lastEntry);
    if ( ret != StatusCode::kOK ) {
        return ret;
    }
    if (lastEntry.empty()) {
        return StatusCode::kCloneFileNameIllegal;
    }

    FileInfo fileInfo;
    ret = LookUpFile(parentFileInfo, lastEntry, &fileInfo);
    if (ret == StatusCode::kOK) {
        return StatusCode::kFileExists;
    }

    if (ret != StatusCode::kFileNotExists) {
         return ret;
    } else {
        InodeID inodeID;
        if (InodeIDGenerator_->GenInodeID(&inodeID) != true) {
            LOG(ERROR) << "CreateCloneFile filename = " << fileName
                << ", GenInodeID error";
            return StatusCode::kStorageError;
        }

        fileInfo.set_id(inodeID);
        fileInfo.set_parentid(parentFileInfo.id());

        fileInfo.set_filename(lastEntry);

        fileInfo.set_filetype(filetype);
        fileInfo.set_owner(owner);

        fileInfo.set_chunksize(chunksize);
        fileInfo.set_segmentsize(defaultSegmentSize_);
        fileInfo.set_length(length);
        fileInfo.set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());

        fileInfo.set_seqnum(seq);
        fileInfo.set_clonesource(cloneSource);
        fileInfo.set_clonelength(cloneLength);

        fileInfo.set_filestatus(FileStatus::kFileCloning);
        fileInfo.set_stripeunit(stripeUnit);
        fileInfo.set_stripecount(stripeCount);

        ret = PutFile(fileInfo);
        if (ret == StatusCode::kOK && retFileInfo != nullptr) {
            *retFileInfo = fileInfo;
        }
        return ret;
    }
}


StatusCode CurveFS::SetCloneFileStatus(const std::string &filename,
                            uint64_t fileID,
                            FileStatus fileStatus) {
    std::string lastEntry;
    FileInfo parentFileInfo;
    auto ret = WalkPath(filename, &parentFileInfo, &lastEntry);
    if ( ret != StatusCode::kOK ) {
        if (ret == StatusCode::kNotDirectory) {
            return StatusCode::kFileNotExists;
        }
        return ret;
    } else {
        if (lastEntry.empty()) {
            return StatusCode::kCloneFileNameIllegal;
        }

        FileInfo fileInfo;
        StatusCode ret = LookUpFile(parentFileInfo, lastEntry, &fileInfo);

        if (ret != StatusCode::kOK) {
            return ret;
        } else {
            if (fileInfo.filetype() != FileType::INODE_PAGEFILE) {
                return StatusCode::kNotSupported;
            }
        }

        if (fileID !=  kUnitializedFileID && fileID != fileInfo.id()) {
            LOG(WARNING) << "SetCloneFileStatus, filename = " << filename
                << "fileID not Matched, src fileID = " << fileID
                << ", stored fileID = " << fileInfo.id();
            return StatusCode::kFileIdNotMatch;
        }

        switch (fileInfo.filestatus()) {
            case kFileCloning:
                if (fileStatus == kFileCloneMetaInstalled ||
                    fileStatus == kFileCloning) {
                    // noop
                } else {
                    return kCloneStatusNotMatch;
                }
                break;
            case kFileCloneMetaInstalled:
                if (fileStatus == kFileCloned ||
                    fileStatus == kFileCloneMetaInstalled ) {
                    // noop
                } else {
                    return kCloneStatusNotMatch;
                }
                break;
            case kFileCloned:
                if (fileStatus == kFileCloned ||
                    fileStatus == kFileBeingCloned) {
                    // noop
                } else {
                    return kCloneStatusNotMatch;
                }
                break;
            case kFileCreated:
                if (fileStatus != kFileCreated &&
                    fileStatus != kFileBeingCloned) {
                    return kCloneStatusNotMatch;
                }
                break;
            case kFileBeingCloned:
                if (fileStatus != kFileBeingCloned &&
                    fileStatus != kFileCreated &&
                    fileStatus != kFileCloned) {
                    return kCloneStatusNotMatch;
                }
                break;
            default:
                return kCloneStatusNotMatch;
        }

        fileInfo.set_filestatus(fileStatus);

        return PutFile(fileInfo);
    }
}

StatusCode CurveFS::CheckPathOwnerInternal(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              std::string *lastEntry,
                              uint64_t *parentID) {
    std::vector<std::string> paths;
    ::curve::common::SplitString(filename, "/", &paths);

    // owner verification not allowed for the root directory
    if ( paths.size() == 0 ) {
        return StatusCode::kOwnerAuthFail;
    }

    *lastEntry = paths.back();
    uint64_t tempParentID = rootFileInfo_.id();

    for (uint32_t i = 0; i < paths.size() - 1; i++) {
        FileInfo  fileInfo;
        auto ret = storage_->GetFile(tempParentID, paths[i], &fileInfo);

        if (ret ==  StoreStatus::OK) {
            if (fileInfo.filetype() !=  FileType::INODE_DIRECTORY) {
                LOG(INFO) << fileInfo.filename() << " is not an directory";
                return StatusCode::kNotDirectory;
            }

            if (fileInfo.owner() != owner) {
                LOG(ERROR) << fileInfo.filename() << " auth fail, owner = "
                           << owner;
                return StatusCode::kOwnerAuthFail;
            }
        } else if (ret == StoreStatus::KeyNotExist) {
            LOG(WARNING) << paths[i] << " not exist";
            return StatusCode::kFileNotExists;
        } else {
            LOG(ERROR) << "GetFile " << paths[i] << " error, errcode = " << ret;
            return StatusCode::kStorageError;
        }
        tempParentID =  fileInfo.id();
    }

    *parentID = tempParentID;
    return StatusCode::kOK;
}

StatusCode CurveFS::CheckDestinationOwner(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              uint64_t date) {
    if (owner.empty()) {
        LOG(ERROR) << "file owner is empty, filename = " << filename
                   << ", owner = " << owner;
        return StatusCode::kOwnerAuthFail;
    }

    StatusCode ret = StatusCode::kOwnerAuthFail;

    if (!CheckDate(date)) {
        LOG(ERROR) << "check date fail, request is staled.";
        return ret;
    }

    // for root user, identity verification with signature is required
    // no more verification is required for root user
    if (owner == GetRootOwner()) {
        ret = CheckSignature(owner, signature, date)
              ? StatusCode::kOK : StatusCode::kOwnerAuthFail;
        LOG_IF(ERROR, ret == StatusCode::kOwnerAuthFail)
              << "check root owner fail, signature auth fail.";
        return ret;
    }

    std::string lastEntry;
    uint64_t parentID;
    // verify the owner of all levels of directories
    ret = CheckPathOwnerInternal(filename, owner, signature,
                                 &lastEntry, &parentID);

    if (ret != StatusCode::kOK) {
        return ret;
    }

    // if the file exists, verify the owner, if not, return kOK
    FileInfo  fileInfo;
    auto ret1 = storage_->GetFile(parentID, lastEntry, &fileInfo);

    if (ret1 == StoreStatus::OK) {
        if (fileInfo.owner() != owner) {
            return StatusCode::kOwnerAuthFail;
        }
        return StatusCode::kOK;
    } else if  (ret1 == StoreStatus::KeyNotExist) {
        return StatusCode::kOK;
    } else {
        return StatusCode::kStorageError;
    }
}

StatusCode CurveFS::CheckPathOwner(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              uint64_t date) {
    if (owner.empty()) {
        LOG(ERROR) << "file owner is empty, filename = " << filename
                   << ", owner = " << owner;
        return StatusCode::kOwnerAuthFail;
    }

    StatusCode ret = StatusCode::kOwnerAuthFail;

    if (!CheckDate(date)) {
        LOG(ERROR) << "check date fail, request is staled.";
        return ret;
    }

    // for root user, identity verification with signature is required
    // no more verification is required for root user
    if (owner == GetRootOwner()) {
        ret = CheckSignature(owner, signature, date)
              ? StatusCode::kOK : StatusCode::kOwnerAuthFail;
        LOG_IF(ERROR, ret == StatusCode::kOwnerAuthFail)
              << "check root owner fail, signature auth fail.";
        return ret;
    }

    std::string lastEntry;
    uint64_t parentID;
    return CheckPathOwnerInternal(filename, owner, signature,
                                    &lastEntry, &parentID);
}

StatusCode CurveFS::CheckRootOwner(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              uint64_t date) {
    if (owner.empty()) {
        LOG(ERROR) << "file owner is empty, filename = " << filename
                   << ", owner = " << owner;
        return StatusCode::kOwnerAuthFail;
    }

    StatusCode ret = StatusCode::kOwnerAuthFail;

    if (!CheckDate(date)) {
        LOG(ERROR) << "check date fail, request is staled.";
        return ret;
    }

    if (owner != GetRootOwner()) {
        LOG(ERROR) << "check root owner fail, owner is :" << owner;
        return ret;
    }

    // use signature to check root user identity
    ret = CheckSignature(owner, signature, date)
            ? StatusCode::kOK : StatusCode::kOwnerAuthFail;
    LOG_IF(ERROR, ret == StatusCode::kOwnerAuthFail)
            << "check root owner fail, signature auth fail.";
    return ret;
}

StatusCode CurveFS::CheckFileOwner(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              uint64_t date) {
    if (owner.empty()) {
        LOG(ERROR) << "file owner is empty, filename = " << filename
                   << ", owner = " << owner;
        return StatusCode::kOwnerAuthFail;
    }

    StatusCode ret = StatusCode::kOwnerAuthFail;

    if (!CheckDate(date)) {
        LOG(ERROR) << "check date fail, request is staled.";
        return ret;
    }

    // for root user, identity verification with signature is required
    // no more verification is required for root user
    if (owner == GetRootOwner()) {
        ret = CheckSignature(owner, signature, date)
              ? StatusCode::kOK : StatusCode::kOwnerAuthFail;
        LOG_IF(ERROR, ret == StatusCode::kOwnerAuthFail)
              << "check root owner fail, signature auth fail.";
        return ret;
    }

    std::string lastEntry;
    uint64_t parentID;
    ret = CheckPathOwnerInternal(filename, owner, signature,
                                 &lastEntry, &parentID);

    if (ret != StatusCode::kOK) {
        return ret;
    }

    FileInfo  fileInfo;
    auto ret1 = storage_->GetFile(parentID, lastEntry, &fileInfo);

    if (ret1 == StoreStatus::OK) {
        if (fileInfo.owner() != owner) {
            return StatusCode::kOwnerAuthFail;
        }
        return StatusCode::kOK;
    } else if  (ret1 == StoreStatus::KeyNotExist) {
        return StatusCode::kFileNotExists;
    } else {
        return StatusCode::kStorageError;
    }
}

StatusCode CurveFS::CheckEpoch(const std::string &filename,
                               uint64_t epoch) {
    FileInfo fileInfo;
    auto ret = GetFileInfo(filename, &fileInfo);
    if (ret != StatusCode::kOK) {
        LOG(INFO) << "get source file error, errCode = " << ret;
        return  ret;
    }
    if (fileInfo.has_epoch() && fileInfo.epoch() > epoch) {
        LOG(ERROR) << "Check Epoch Failed, fileInfo.epoch: "
                   << fileInfo.epoch()
                   << ", epoch: " << epoch;
        return StatusCode::kEpochTooOld;
    }
    return StatusCode::kOK;
}

// kStaledRequestTimeIntervalUs represents the expiration time of the request
// to prevent the request from being intercepted and played back
bool CurveFS::CheckDate(uint64_t date) {
    uint64_t current = TimeUtility::GetTimeofDayUs();

    // prevent time shift between machines
    uint64_t interval = (date > current) ? date - current : current - date;

    return interval < kStaledRequestTimeIntervalUs;
}

bool CurveFS::CheckSignature(const std::string& owner,
                             const std::string& signature,
                             uint64_t date) {
    std::string str2sig = Authenticator::GetString2Signature(date, owner);
    std::string sig = Authenticator::CalcString2Signature(str2sig,
                                                rootAuthOptions_.rootPassword);
    return signature == sig;
}

StatusCode CurveFS::ListClient(bool listAllClient,
                               std::vector<ClientInfo>* clientInfos) {
    std::set<butil::EndPoint> allClients = fileRecordManager_->ListAllClient();

    for (const auto &c : allClients) {
        clientInfos->emplace_back(EndPointToClientInfo(c));
    }

    return StatusCode::kOK;
}

StatusCode CurveFS::CheckHasCloneRely(const std::string & filename,
                                             const std::string &owner,
                                             bool *isHasCloneRely) {
    CloneRefStatus refStatus;
    std::vector<snapshotcloneclient::DestFileInfo> fileCheckList;
    StatusCode ret = snapshotCloneClient_->GetCloneRefStatus(filename,
                                            owner, &refStatus, &fileCheckList);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "delete file, check file clone ref fail,"
                    << "filename = " << filename
                    << ", ret = " << ret;
        return ret;
    }
    bool hasCloneRef = false;
    if (refStatus == CloneRefStatus::kHasRef) {
        hasCloneRef = true;
    } else if (refStatus == CloneRefStatus::kNoRef) {
        hasCloneRef = false;
    } else {
        int recordNum = fileCheckList.size();
        for (int i = 0; i < recordNum; i++) {
            FileInfo  destFileInfo;
            StatusCode ret2 = GetFileInfo(fileCheckList[i].filename,
                                            &destFileInfo);
            if (ret2 == StatusCode::kFileNotExists) {
                continue;
            } else if (ret2 == StatusCode::kOK) {
                if (destFileInfo.id() == fileCheckList[i].inodeid) {
                    hasCloneRef = true;
                    break;
                }
            } else {
                LOG(ERROR) << "CheckHasCloneRely, check clonefile exist fail"
                            << ", filename = " << filename
                            << ", clonefile = " << fileCheckList[i].filename
                            << ", ret = " << ret2;
                return ret2;
            }
        }
    }

    *isHasCloneRely = hasCloneRef;
    return StatusCode::kOK;
}

StatusCode CurveFS::ListCloneSourceFileSegments(
    const FileInfo* fileInfo, CloneSourceSegment* cloneSourceSegment) const {
    if (fileInfo->filestatus() != FileStatus::kFileCloneMetaInstalled) {
        LOG(INFO) << fileInfo->filename()
                  << " hash clone source, but file status is "
                  << FileStatus_Name(fileInfo->filestatus())
                  << ", return empty CloneSourceSegment";
        return StatusCode::kOK;
    }

    if (!cloneSourceSegment) {
        LOG(ERROR) << "OpenFile failed, file has clone source, but "
                      "cloneSourceSegments is nullptr, filename = "
                   << fileInfo->filename();
        return StatusCode::kParaError;
    }

    FileInfo cloneSourceFileInfo;
    StatusCode ret = GetFileInfo(fileInfo->clonesource(), &cloneSourceFileInfo);
    if (ret != StatusCode::kOK) {
        LOG(ERROR)
            << "OpenFile failed, Get clone source file info failed, ret = "
            << StatusCode_Name(ret) << ", filename = " << fileInfo->filename()
            << ", clone source = " << fileInfo->clonesource()
            << ", file status = " << FileStatus_Name(fileInfo->filestatus());
        return ret;
    }

    std::vector<PageFileSegment> segments;
    StoreStatus status =
        storage_->ListSegment(cloneSourceFileInfo.id(), &segments);
    if (status != StoreStatus::OK) {
        LOG(ERROR) << "OpenFile failed, list clone source segment failed, "
                      "filename = "
                   << fileInfo->filename()
                   << ", source file name = " << fileInfo->clonesource()
                   << ", ret = " << status;
        return StatusCode::kStorageError;
    }

    cloneSourceSegment->set_segmentsize(fileInfo->segmentsize());

    if (segments.empty()) {
        LOG(WARNING) << "Clone source file has no segments, filename = "
                     << fileInfo->clonesource();
    } else {
        for (const auto& segment : segments) {
            cloneSourceSegment->add_allocatedsegmentoffset(
                segment.startoffset());
        }
    }

    return StatusCode::kOK;
}

StatusCode CurveFS::FindFileMountPoint(const std::string& fileName,
                                       std::vector<ClientInfo>* clientInfos) {
    std::vector<butil::EndPoint> mps;
    auto res = fileRecordManager_->FindFileMountPoint(fileName, &mps);

    if (res) {
        clientInfos->reserve(mps.size());
        for (auto& mp : mps) {
            clientInfos->emplace_back(EndPointToClientInfo(mp));
        }
        return StatusCode::kOK;
    }

    return StatusCode::kFileNotExists;
}

StatusCode CurveFS::ListVolumesOnCopyset(
                        const std::vector<common::CopysetInfo>& copysets,
                        std::vector<std::string>* fileNames) {
    std::vector<FileInfo> files;
    StatusCode ret = ListAllFiles(ROOTINODEID, &files);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "List all files in root directory fail";
        return ret;
    }
    std::map<LogicalPoolIdType, std::set<CopySetIdType>> copysetMap;
    for (const auto& copyset : copysets) {
        copysetMap[copyset.logicalpoolid()].insert(copyset.copysetid());
    }
    for (const auto& file : files) {
        std::vector<PageFileSegment> segments;
        StoreStatus ret = storage_->ListSegment(file.id(), &segments);
        if (ret != StoreStatus::OK) {
            LOG(ERROR) << "List segments of " << file.filename() << " fail";
            return StatusCode::kStorageError;
        }
        bool found = false;
        for (const auto& segment : segments) {
            if (copysetMap.find(segment.logicalpoolid()) == copysetMap.end()) {
                continue;
            }
            for (int i = 0; i < segment.chunks_size(); i++) {
                auto copysetId = segment.chunks(i).copysetid();
                if (copysetMap[segment.logicalpoolid()].count(copysetId) != 0) {
                    fileNames->emplace_back(file.filename());
                    found = true;
                    break;
                }
            }
            if (found) {
                break;
            }
        }
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::ListAllFiles(uint64_t inodeId,
                                 std::vector<FileInfo>* files) {
    std::vector<FileInfo> tempFiles;
    StoreStatus ret = storage_->ListFile(inodeId, inodeId + 1, &tempFiles);
    if (ret != StoreStatus::OK) {
        return StatusCode::kStorageError;
    }
    for (const auto& file : tempFiles) {
        if (file.filetype() == FileType::INODE_PAGEFILE) {
            files->emplace_back(file);
        } else if (file.filetype() == FileType::INODE_DIRECTORY) {
            std::vector<FileInfo> tempFiles2;
            StatusCode ret = ListAllFiles(file.id(), &tempFiles2);
            if (ret == StatusCode::kOK) {
                files->insert(files->end(), tempFiles2.begin(),
                              tempFiles2.end());
            } else {
                LOG(ERROR) << "ListAllFiles in file " << inodeId << " fail";
                return ret;
            }
        }
    }
    return StatusCode::kOK;
}

StatusCode CurveFS::BuildEpochMap(::google::protobuf::Map<
    ::google::protobuf::uint64, ::google::protobuf::uint64>  *epochMap) {
    epochMap->clear();
    std::vector<FileInfo> files;
    StatusCode ret = ListAllFiles(ROOTINODEID, &files);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "List all files in root directory fail";
        return ret;
    }
    for (const auto& file : files) {
        if (file.has_epoch()) {
            epochMap->insert({file.id(), file.epoch()});
        }
    }
    return ret;
}

uint64_t CurveFS::GetOpenFileNum() {
    if (fileRecordManager_ == nullptr) {
        return 0;
    }

    return fileRecordManager_->GetOpenFileNum();
}

uint64_t CurveFS::GetDefaultChunkSize() {
    return defaultChunkSize_;
}

uint64_t CurveFS::GetDefaultSegmentSize() {
    return defaultSegmentSize_;
}

uint64_t CurveFS::GetMinFileLength() {
    return minFileLength_;
}

uint64_t CurveFS::GetMaxFileLength() {
    return maxFileLength_;
}

StatusCode CurveFS::CheckStripeParam(uint64_t stripeUnit,
                           uint64_t stripeCount) {
    if ((stripeUnit == 0) && (stripeCount == 0 )) {
        return StatusCode::kOK;
    }

    if ((stripeUnit && !stripeCount) ||
    (!stripeUnit && stripeCount)) {
        LOG(ERROR) << "can't just one is zero. stripeUnit:"
        << stripeUnit << ",stripeCount:" << stripeCount;
        return StatusCode::kParaError;
    }

    if (stripeUnit > defaultChunkSize_) {
        LOG(ERROR) << "stripeUnit more than chunksize.stripeUnit:"
                                                   << stripeUnit;
        return StatusCode::kParaError;
    }

    if ((defaultChunkSize_ % stripeUnit != 0) ||
                 (defaultChunkSize_ % stripeCount != 0)) {
        LOG(ERROR) << "is not divisible by chunksize. stripeUnit:"
           << stripeUnit << ",stripeCount:" << stripeCount;
        return StatusCode::kParaError;
    }

     // chunkserver check req offset and len align as 4k,
     // such as ChunkServiceImpl::CheckRequestOffsetAndLength
    if (stripeUnit % 4096 != 0) {
        LOG(ERROR) << "stripeUnit is not aligned as 4k. stripeUnit:"
           << stripeUnit << ",stripeCount:" << stripeCount;
        return StatusCode::kParaError;
    }

    return StatusCode::kOK;
}

CurveFS &kCurveFS = CurveFS::GetInstance();

int ChunkServerRegistInfoBuilderImpl::BuildEpochMap(::google::protobuf::Map<
    ::google::protobuf::uint64,
    ::google::protobuf::uint64> *epochMap) {
    if (cfs_ == nullptr) {
        return -1;
    }
    StatusCode ret = cfs_->BuildEpochMap(epochMap);
    if (ret != StatusCode::kOK) {
        LOG(ERROR) << "BuildEpochMap failed, statusCode: " << ret
                   << ", StatusCode_Name: " << StatusCode_Name(ret);
        return -1;
    }
    return 0;
}

uint64_t GetOpenFileNum(void *varg) {
    CurveFS *curveFs = reinterpret_cast<CurveFS *>(varg);
    return curveFs->GetOpenFileNum();
}

bvar::PassiveStatus<uint64_t> g_open_file_num_bvar(
                        CURVE_MDS_CURVEFS_METRIC_PREFIX, "open_file_num",
                        GetOpenFileNum, &kCurveFS);
}   // namespace mds
}   // namespace curve
