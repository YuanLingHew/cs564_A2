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

void BufMgr::advanceClock() {
  clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId& frame) {

  int pinned_frame = 0;
  while (pinned_frame < numBufs) {
    
    advanceClock();
    BufDesc *buf_desc = &bufDescTable[clockHand];
    
    // page is not valid, set frame
    if (!buf_desc->valid) break;

    // refbit is set, clear and continue
    if (buf_desc->refbit) {
      buf_desc->refbit = false;
      continue;
    }
    
    // page is pinned, continue
    if (buf_desc->pinCnt > 0) {
      pinned_frame = pinned_frame + 1;
      continue;
    }

    // dirty bit is set
    if (buf_desc->dirty) {

      File file = buf_desc->file;
      PageId pageNo = buf_desc->pageNo;

      // flush page to disks
      Page page = bufPool[clockHand];
      file.writePage(page);

      // delete from buffer
      hashTable.remove(file, pageNo);
      buf_desc->clear();
    }

    break;
  }

  // all frame are pinned, throw exception
  if (pinned_frame == numBufs) {
    throw BufferExceededException();
  }

  // set frame
  frame = clockHand;
}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {

  FrameId frame_id;
  try {

    // look up frame
    hashTable.lookup(file, pageNo, frame_id);

    // modify frame stat
    BufDesc *buf_desc = &bufDescTable[frame_id];
    buf_desc->refbit = 1;
    buf_desc->pinCnt += 1;

  } catch (HashNotFoundException e) {

    // 1. allocate buffer frame
    allocPage(frame_id); // allocBuf here

    // 2. read page from disk
    Page new_page = file.readPage(pageNo);
    bufPool[frame_id] = new_page;

    // 3. insert page into hash table
    hashTable.insert(file, pageNo, frame_id);

    // 4. set frame
    bufDescTable[frame_id].Set(file, pageNo);
  }

  page = &bufPool[frame_id];
}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {

    FrameId frame_no; // obtain frame number by reference using file, pageNo

    try{
      // Find the pageId in the frame of the buffer pool
      hashTable.lookup(file, pageNo, frame_no);
      // Retrieve BufDesc
      BufDesc f = bufDescTable[frame_no];
      // If pin count is already zero, throw PAGENOTPINNED
      if(f.pinCnt == 0)
          throw PageNotPinnedException(file.filename(), pageNo, frame_no);
      
      // Decrement pin count
      f.pinCnt--;
    
      // If dirty is true, set the dirty bit
      if(dirty)
        f.dirty = 1;

    }catch(HashNotFoundException e){
      // If pageId in the frame is not in the buffer pool, do nothing
    }
    
}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {


}

void BufMgr::flushFile(File& file) {}

void BufMgr::disposePage(File& file, const PageId PageNo) {}

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
