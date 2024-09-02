#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>
#include <iostream>
using namespace std;

// the declarations for these functions can be found in "BlockBuffer.h"

BlockBuffer::BlockBuffer(int blockNum)
{
    // initialise this.blockNum with the argument
    this->blockNum = blockNum;
}

BlockBuffer::BlockBuffer(char blockType)
{
    int blockTypeNum;
    if (blockType == 'R')
    {
        blockTypeNum = REC;
    }
    else if (blockType == 'I')
    {
        blockTypeNum = IND_INTERNAL;
    }
    else if (blockType == 'L')
    {
        blockTypeNum = IND_LEAF;
    }
    else
    {
        blockTypeNum = UNUSED_BLK;
    }
    int blockNum = getFreeBlock(blockTypeNum);

    this->blockNum = blockNum;
    if (blockNum < 0 || blockNum >= DISK_BLOCKS)
        return;
}

// calls the parent class constructor
RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum)
{
}
RecBuffer::RecBuffer() : BlockBuffer('R') {}
// call parent non-default constructor with 'R' denoting record block.

// call the corresponding parent constructor
IndBuffer::IndBuffer(char blockType) : BlockBuffer(blockType) {}

// call the corresponding parent constructor
IndBuffer::IndBuffer(int blockNum) : BlockBuffer(blockNum) {}

IndInternal::IndInternal() : IndBuffer('I') {}
// call the corresponding parent constructor
// 'I' used to denote IndInternal.

IndInternal::IndInternal(int blockNum) : IndBuffer(blockNum) {}
// call the corresponding parent constructor

IndLeaf::IndLeaf() : IndBuffer('L') {} // this is the way to call parent non-default constructor.
                                       // 'L' used to denote IndLeaf.

// this is the way to call parent non-default constructor.
IndLeaf::IndLeaf(int blockNum) : IndBuffer(blockNum) {}

int BlockBuffer::getBlockNum()
{
    // return corresponding block number.
    return this->blockNum;
}

// load the block header into the argument pointer
int BlockBuffer::getHeader(struct HeadInfo *head)
{

    unsigned char *bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
    {
        return ret; // return any errors that might have occured in the process
    }

    HeadInfo *header = (HeadInfo *)bufferPtr;

    head->numSlots = header->numSlots;
    head->numEntries = header->numEntries;
    head->numAttrs = header->numAttrs;
    head->lblock = header->lblock;
    head->rblock = header->rblock;
    head->pblock = header->pblock;

    return SUCCESS;
}

int BlockBuffer::setHeader(struct HeadInfo *head)
{
    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if (ret != SUCCESS)
        return ret;

    HeadInfo *header = (HeadInfo *)bufferPtr;

    header->numSlots = head->numSlots;
    header->numEntries = head->numEntries;
    header->numAttrs = head->numAttrs;
    header->lblock = head->lblock;
    header->rblock = head->rblock;
    header->pblock = head->pblock;

    return StaticBuffer::setDirtyBit(this->blockNum);
}

// load the record at slotNum into the argument pointer
int RecBuffer::getRecord(union Attribute *rec, int slotNum)
{
    struct HeadInfo head;
    this->getHeader(&head);
    // get the header using this.getHeader() function

    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;

    unsigned char *bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
    {
        return ret;
    }

    /* record at slotNum will be at offset HEADER_SIZE + slotMapSize + (recordSize * slotNum)
       - each record will have size attrCount * ATTR_SIZE
       - slotMap will be of size slotCount
    */

    int recordSize = attrCount * ATTR_SIZE;

    int offset = HEADER_SIZE + slotCount + (recordSize * slotNum);
    // proof that each record block only contain records specific to a single relation
    // SlotMap is generated dynamically based on the number of attributes so, here when we are acessing records based on this calculation.
    // This calculation is based on the assumption that the slotMap is at the beginning of the block and the records are stored after the slotMap
    // so slot map must be constant and also number of attributes. So we cannot keep records of different relations in the same block.

    // unsigned char *slotPointer = &buffer/* calculate buffer + offset */;

    // load the record into the rec data structure
    memcpy(rec, bufferPtr + offset, recordSize);

    return SUCCESS;
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum)
{
    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
    // return the value returned by the call.
    if (ret != SUCCESS)
    {
        return ret;
    }

    /* get the header of the block using the getHeader() function */
    struct HeadInfo head;
    this->getHeader(&head);

    // get number of attributes in the block.
    int numAttrs = head.numAttrs;

    // get the number of slots in the block.
    int numSlots = head.numSlots;

    // if input slotNum is not in the permitted range return E_OUTOFBOUND.
    if (slotNum >= numSlots || slotNum < 0)
    {
        return E_OUTOFBOUND;
    }

    /* offset bufferPtr to point to the beginning of the record at required
       slot. the block contains the header, the slotmap, followed by all
       the records. so, for example,
       record at slot x will be at bufferPtr + HEADER_SIZE + (x*recordSize)
       copy the record from `rec` to buffer using memcpy
       (hint: a record will be of size ATTR_SIZE * numAttrs)
    */
    int recordSize = numAttrs * ATTR_SIZE;
    int offset = HEADER_SIZE + numSlots + (recordSize * slotNum);
    memcpy(bufferPtr + offset, rec, recordSize);

    // update dirty bit using setDirtyBit()
    StaticBuffer::setDirtyBit(this->blockNum);

    /* (the above function call should not fail since the block is already
       in buffer and the blockNum is valid. If the call does fail, there
       exists some other issue in the code) */

    // return SUCCESS
    return SUCCESS;
}

/*
Used to load a block to the buffer and get a pointer to it.
NOTE: this function expects the caller to allocate memory for the argument
*/
/* NOTE: This function will NOT check if the block has been initialised as a
   record or an index block. It will copy whatever content is there in that
   disk block to the buffer.
   Also ensure that all the methods accessing and updating the block's data
   should call the loadBlockAndGetBufferPtr() function before the access or
   update is done. This is because the block might not be present in the
   buffer due to LRU buffer replacement. So, it will need to be bought back
   to the buffer before any operations can be done.
 */
int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **bufferPtr)
{
    int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

    if (bufferNum == E_BLOCKNOTINBUFFER)
    {
        bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

        if (bufferNum == E_OUTOFBOUND)
            return E_OUTOFBOUND;

        Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
    }
    else
    {
        for (int i = 0; i < BUFFER_CAPACITY; i++)
        {
            if (!StaticBuffer::metainfo[i].free)
                StaticBuffer::metainfo[i].timeStamp++;
        }

        StaticBuffer::metainfo[bufferNum].timeStamp = 0;
    }

    *bufferPtr = StaticBuffer::blocks[bufferNum];

    return SUCCESS;
}
/* used to get the slotmap from a record block
NOTE: this function expects the caller to allocate memory for `*slotMap`
*/
int RecBuffer::getSlotMap(unsigned char *slotMap)
{
    unsigned char *bufferPtr;

    // get the starting address of the buffer containing the block using loadBlockAndGetBufferPtr().
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
    {
        return ret;
    }

    struct HeadInfo head;
    // get the header of the block using getHeader() function
    ret = BlockBuffer::getHeader(&head);

    int slotCount = head.numSlots;

    // get a pointer to the beginning of the slotmap in memory by offsetting HEADER_SIZE
    unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;

    // copy the values from `slotMapInBuffer` to `slotMap` (size is `slotCount`)
    memcpy(slotMap, slotMapInBuffer, slotCount);

    return SUCCESS;
}

int RecBuffer::setSlotMap(unsigned char *slotMap)
{
    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block using
       loadBlockAndGetBufferPtr(&bufferPtr). */
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
    // return the value returned by the call.
    if (ret != SUCCESS)
    {
        return ret;
    }

    // get the header of the block using the getHeader() function
    struct HeadInfo head;
    this->getHeader(&head);

    int numSlots = head.numSlots;

    // the slotmap starts at bufferPtr + HEADER_SIZE. Copy the contents of the
    // argument `slotMap` to the buffer replacing the existing slotmap.
    // Note that size of slotmap is `numSlots`
    unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;
    memcpy(slotMapInBuffer, slotMap, numSlots);

    // update dirty bit using StaticBuffer::setDirtyBit
    // if setDirtyBit failed, return the value returned by the call
    return StaticBuffer::setDirtyBit(this->blockNum);
}

int BlockBuffer::getFreeBlock(int blockType)
{
    int freeBlock = -1;
    for (int i = 0; i < DISK_BLOCKS; i++)
    {
        if (StaticBuffer::blockAllocMap[i] == UNUSED_BLK)
        {
            freeBlock = i;
            break;
        }
    }

    if (freeBlock == -1)
    {
        return E_DISKFULL;
    }

    this->blockNum = freeBlock;

    int freeBuffer = StaticBuffer::getFreeBuffer(freeBlock);

    HeadInfo header;
    header.pblock = -1;
    header.lblock = -1;
    header.rblock = -1;
    header.numEntries = 0;
    header.numAttrs = 0;
    header.numSlots = 0;

    this->setHeader(&header);
    this->setBlockType(blockType);

    return freeBlock;
}

void BlockBuffer::releaseBlock()
{

    if (blockNum < 0 || blockNum >= DISK_BLOCKS || StaticBuffer::blockAllocMap[blockNum] == UNUSED_BLK)
    {
        return;
    }

    int bufferNum = StaticBuffer::getBufferNum(blockNum);

    if (bufferNum == E_BLOCKNOTINBUFFER)
    {
        return;
    }

    StaticBuffer::metainfo[bufferNum].free = true;

    StaticBuffer::blockAllocMap[blockNum] = UNUSED_BLK;

    this->blockNum = INVALID_BLOCKNUM;
}

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType)
{

    double diff;
    if (attrType == STRING)
        diff = strcmp(attr1.sVal, attr2.sVal);
    else
        diff = attr1.nVal - attr2.nVal;

    if (diff > 0)
        return 1;
    else if (diff < 0)
        return -1;
    else
        return 0;
}

int IndInternal::getEntry(void *ptr, int indexNum)
{
    // if the indexNum is not in the valid range of [0, MAX_KEYS_INTERNAL-1]
    //     return E_OUTOFBOUND.
    if (indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL)
    {
        return E_OUTOFBOUND;
    }

    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
    //     return the value returned by the call.
    if (ret != SUCCESS)
    {
        return ret;
    }

    // typecast the void pointer to an internal entry pointer
    struct InternalEntry *internalEntry = (struct InternalEntry *)ptr;

    /*
    - copy the entries from the indexNum`th entry to *internalEntry
    - make sure that each field is copied individually as in the following code
    - the lChild and rChild fields of InternalEntry are of type int32_t
    - int32_t is a type of int that is guaranteed to be 4 bytes across every
      C++ implementation. sizeof(int32_t) = 4
    */

    /* the indexNum'th entry will begin at an offset of
       HEADER_SIZE + (indexNum * (sizeof(int) + ATTR_SIZE) )         [why?]
       from bufferPtr */
    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * 20);

    memcpy(&(internalEntry->lChild), entryPtr, sizeof(int32_t));
    memcpy(&(internalEntry->attrVal), entryPtr + 4, sizeof(Attribute));
    memcpy(&(internalEntry->rChild), entryPtr + 20, sizeof(int32_t));

    // return SUCCESS.
    return SUCCESS;
}

int IndLeaf::getEntry(void *ptr, int indexNum)
{

    // if the indexNum is not in the valid range of [0, MAX_KEYS_LEAF-1]
    //     return E_OUTOFBOUND.
    if (indexNum < 0 || indexNum >= MAX_KEYS_LEAF)
    {
        return E_OUTOFBOUND;
    }

    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
    //     return the value returned by the call.
    if (ret != SUCCESS)
    {
        return ret;
    }

    // copy the indexNum'th Index entry in buffer to memory ptr using memcpy

    /* the indexNum'th entry will begin at an offset of
       HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE)  from bufferPtr */
    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
    // memcpy((struct Index *)ptr, entryPtr, LEAF_ENTRY_SIZE);
    struct Index *index = (struct Index *)ptr;
    memcpy(&(index->attrVal), entryPtr, sizeof(Attribute));
    memcpy(&(index->block), entryPtr + 16, 4);
    memcpy(&(index->slot), entryPtr + 20, 4);

    // return SUCCESS
    return SUCCESS;
}

int IndInternal::setEntry(void *ptr, int indexNum)
{
    // if the indexNum is not in the valid range of [0, MAX_KEYS_INTERNAL-1]
    //     return E_OUTOFBOUND.
    if (indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL)
    {
        return E_OUTOFBOUND;
    }

    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
    //     return the value returned by the call.
    if (ret != SUCCESS)
    {
        return ret;
    }

    // typecast the void pointer to an internal entry pointer
    struct InternalEntry *internalEntry = (struct InternalEntry *)ptr;

    /*
    - copy the entries from *internalEntry to the indexNum`th entry
    - make sure that each field is copied individually as in the following code
    - the lChild and rChild fields of InternalEntry are of type int32_t
    - int32_t is a type of int that is guaranteed to be 4 bytes across every
      C++ implementation. sizeof(int32_t) = 4
    */

    /* the indexNum'th entry will begin at an offset of
       HEADER_SIZE + (indexNum * (sizeof(int) + ATTR_SIZE) )         [why?]
       from bufferPtr */

    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * 20);

    memcpy(entryPtr, &(internalEntry->lChild), 4);
    memcpy(entryPtr + 4, &(internalEntry->attrVal), ATTR_SIZE);
    memcpy(entryPtr + 20, &(internalEntry->rChild), 4);

    // update dirty bit using setDirtyBit()
    // if setDirtyBit failed, return the value returned by the call
    return StaticBuffer::setDirtyBit(this->blockNum);
}
int IndLeaf::setEntry(void *ptr, int indexNum)
{
    // if the indexNum is not in the valid range of [0, MAX_KEYS_LEAF-1]
    //     return E_OUTOFBOUND.
    if (indexNum < 0 || indexNum >= MAX_KEYS_LEAF)
        return E_OUTOFBOUND;

    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
    //     return the value returned by the call.
    if (ret != SUCCESS)
    {
        return ret;
    }

    // copy the Index at ptr to indexNum'th entry in the buffer using memcpy

    /* the indexNum'th entry will begin at an offset of
       HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE)  from bufferPtr */
    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
    struct Index *index = (struct Index *)ptr;
    memcpy(entryPtr, &(index->attrVal), sizeof(Attribute));
    memcpy(entryPtr + 16, &(index->block), sizeof(int));
    memcpy(entryPtr + 20, &(index->slot), sizeof(int));

    // update dirty bit using setDirtyBit()
    return StaticBuffer::setDirtyBit(this->blockNum);
    // if setDirtyBit failed, return the value returned by the call
}

int BlockBuffer::setBlockType(int blockType)
{

    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
    // return the value returned by the call.
    if (ret != SUCCESS)
        return ret;

    // store the input block type in the first 4 bytes of the buffer.
    // (hint: cast bufferPtr to int32_t* and then assign it)
    // *((int32_t *)bufferPtr) = blockType;
    *((int32_t *)bufferPtr) = blockType;

    // update the StaticBuffer::blockAllocMap entry corresponding to the
    // object's block number to `blockType`.
    StaticBuffer::blockAllocMap[this->blockNum] = blockType;

    // update dirty bit by calling StaticBuffer::setDirtyBit()
    // if setDirtyBit() failed
    // return the returned value from the call
    return StaticBuffer::setDirtyBit(this->blockNum);
}