/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs),
      hashTable(HASHTABLE_SZ(bufs)),
      bufDescTable(bufs),
      bufPool(bufs) {
  for (FrameId i = 0; i < bufs; i++) {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  clockHand = bufs - 1;
}

void BufMgr::advanceClock() {}

void BufMgr::allocBuf(FrameId& frame) {}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {}

/**
 * @brief Scan bufTable for pages belonging to the file, for each page encountered it should:
 * (a) if the page is dirty, call file.writePage() to flush the page to disk and then set the
 * dirty bit for the page to false,
 * (b) remove the page from the hashtable (whether the page is
 * clean or dirty) and 
 * (c) invoke the Clear() method of BufDesc for the page frame.
 * 
 * @param file 
 * @throws PagePinnedException If some page of the file is pinned
 * @throws BadBufferException If an invalid page belonging to the file is encoutered
 */
void BufMgr::flushFile(File& file) {
  // scan bufDescTable (frames)
  for (auto& bd : bufDescTable) {
    // finds page associated with the file
    if (bd.file == file) {
      // if page of file is pinned
      if (bd.pinCnt > 0){
        throw new PagePinnedException(bd.file.filename(), bd.pageNo, bd.frameNo);
      }
      // if invalid page
      if (!bd.valid){
        throw new BadBufferException(bd.frameNo, bd.dirty, bd.valid, bd.refbit);
      }
      // if page is dirty, flush page to disk and set dirty bit to false
      if (bd.dirty){
        file.writePage(file.readPage(bd.pageNo));
        bd.dirty = false;
      }
      // remove page from hashtable
      hashTable.remove(file, bd.pageNo);
      // invoke clear method
      bd.clear();
    }
  }
}

/**
 * @brief This method deletes a particular page from file. Before deleting the page from file,
 * it makes sure that if the page to be deleted is allocated a frame in the buffer pool, that
 * frame is freed and correspondingly entry from hash table is also removed.
 * 
 * @param file 
 * @param PageNo 
 */
void BufMgr::disposePage(File& file, const PageId PageNo) {
  FrameId frameNo;
  // get frameNo
  hashTable.lookup(file, PageNo, frameNo);
  // finds frame and frees it, remove entry from hashtable
  for (BufDesc bd: bufDescTable){
    if (bd.frameNo == frameNo){
      bd.clear();
      hashTable.remove(file, PageNo);
    }
  }
}

void BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
