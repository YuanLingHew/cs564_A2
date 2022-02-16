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

/**
 * Constructor of BufMgr class
 */
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

/**
 * @brief Advances clock to the next frame in the buffer pool
 */
void BufMgr::advanceClock() {
  clockHand = (clockHand + 1) % numBufs;
}

/**
 * @brief Allocate a free frame.
 *
 * @param frame   Frame reference, frame ID of allocated frame returned
 * via this variable
 * @throws BufferExceededException If no such buffer is found which can be
 * allocated
 */
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

/**
 * @brief Reads the given page from the file into a frame and returns the pointer to
 * page. If the requested page is already present in the buffer pool pointer
 * to that frame is returned otherwise a new frame is allocated from the
 * buffer pool for reading the page.
 *
 * @param file   	File object
 * @param PageNo  Page number in the file to be read
 * @param page  	Reference to page pointer. Used to fetch the Page object
 * in which requested page from file is read in.
 */
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
    allocBuf(frame_id); // allocBuf here

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

/**
 * @brief Unpin a page from memory since it is no longer required for it to remain in
 * memory.
 *
 * @param file   	File object
 * @param PageNo  Page number
 * @param dirty		True if the page to be unpinned needs to be
 * marked dirty
 * @throws  PageNotPinnedException If the page is not already pinned
 */
void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {

  FrameId frameNo; // obtain frame number by reference using file, pageNo

  try {

    // Find the pageId in the frame of the buffer pool
    hashTable.lookup(file, pageNo, frameNo);

    // Retrieve BufDesc
    BufDesc *f = &bufDescTable[frameNo];
    
    // If pin count is already zero, throw PAGENOTPINNED
    if (f->pinCnt == 0)
      throw PageNotPinnedException(file.filename(), pageNo, frameNo);
    
    // Decrement pin count
    f->pinCnt--;
  
    // If dirty is true, set the dirty bit
    if (dirty)
      f->dirty = 1;
    
  } catch (HashNotFoundException e) {
    // If pageId in the frame is not in the buffer pool, do nothing
  }

}

/**
 * @brief Allocates a new, empty page in the file and returns the Page object.
 * The newly allocated page is also assigned a frame in the buffer pool.
 *
 * @param file   	File object
 * @param PageNo  Page number. The number assigned to the page in the file is
 * returned via this reference.
 * @param page  	Reference to page pointer. The newly allocated in-memory
 * Page object is returned via this reference.
 */
void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {

  FrameId frameNo;
  // Allocate an empty page in the specified file
  Page newPage = file.allocatePage();

  // return page number of newly allocated page
  pageNo = newPage.page_number();

  // Call allocBuf() to obtain buffer pool frame
  allocBuf(frameNo);

  // insert entry and call Set() on the frame 
  hashTable.insert(file, pageNo, frameNo);

  // Update frame description
  BufDesc *f = &bufDescTable[frameNo];
  f->Set(file, pageNo);

  // Allocate new allocated page to buffer pool frame
  bufPool[frameNo] = newPage;

  // return new page by reference
  page = &bufPool[frameNo];
}

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
        throw PagePinnedException(bd.file.filename(), bd.pageNo, bd.frameNo);
      }

      // if invalid page
      if (!bd.valid){
        throw BadBufferException(bd.frameNo, bd.dirty, bd.valid, bd.refbit);
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
