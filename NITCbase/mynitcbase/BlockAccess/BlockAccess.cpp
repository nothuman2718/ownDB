#include "BlockAccess.h"

#include <cstring>
#include <iostream>
using namespace std;

RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op)
{
    // TODO: No error handling is done in this function. Should add error handling code.
    // get the previous search index of the relation relId from the relation cache
    // (use RelCacheTable::getSearchIndex() function)
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);

    int block, slot;

    // if the current search index record is invalid(i.e. both block and slot = -1)
    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        // (no hits from previous search; search should start from the
        // first record itself)

        // get the first record block of the relation from the relation cache
        // (use RelCacheTable::getRelCatEntry() function of Cache Layer)
        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId, &relCatEntry);

        block = relCatEntry.firstBlk;
        slot = 0;

        // block = first record block of the relation
        // slot = 0
    }
    else
    {
        // (there is a hit from previous search; search should start from
        // the record next to the search index record)

        // TODO: What if the previous hit is the last record of the relation?
        // How exactly should I move the block and slot pointers in that case?
        // block = search index's block
        // slot = search index's slot + 1
        block = prevRecId.block;
        slot = prevRecId.slot + 1;
    }

    /* The following code searches for the next record in the relation
       that satisfies the given condition
       We start from the record id (block, slot) and iterate over the remaining
       records of the relation
    */
    while (block != -1)
    {
        /* create a RecBuffer object for block (use RecBuffer Constructor for
           existing block) */
        RecBuffer recBuffer(block);

        // get the record with id (block, slot) using RecBuffer::getRecord()
        HeadInfo head;
        recBuffer.getHeader(&head);
        int numAttrs = head.numAttrs;
        Attribute record[numAttrs];
        recBuffer.getRecord(record, slot);

        // get header of the block using RecBuffer::getHeader() function
        // get slot map of the block using RecBuffer::getSlotMap() function

        unsigned char slotMap[head.numSlots];
        recBuffer.getSlotMap(slotMap);

        // If slot >= the number of slots per block(i.e. no more slots in this block)
        if (slot >= head.numSlots)
        {
            // update block = right block of block
            // (use the block header to get the right block of the current block)
            block = head.rblock;
            slot = 0;
            // update slot = 0
            continue; // continue to the beginning of this while loop
        }

        // if slot is free skip the loop
        // (i.e. check if slot'th entry in slot map of block contains SLOT_UNOCCUPIED)
        if (slotMap[slot] == SLOT_UNOCCUPIED)
        {
            // increment slot and continue to the next record slot
            slot++;
            continue;
        }
        // compare record's attribute value to the the given attrVal as below:
        /*
            firstly get the attribute offset for the attrName attribute
            from the attribute cache entry of the relation using
            AttrCacheTable::getAttrCatEntry()
        */
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
        /* use the attribute offset to get the value of the attribute from
           current record */
        Attribute recordAttrVal = record[attrCatEntry.offset];
        int cmpVal; // will store the difference between the attributes
        // set cmpVal using compareAttrs()
        cmpVal = compareAttrs(recordAttrVal, attrVal, attrCatEntry.attrType);

        /* Next task is to check whether this record satisfies the given condition.
           It is determined based on the output of previous comparison and
           the op value received.
           The following code sets the cond variable if the condition is satisfied.
        */
        if (
            (op == NE && cmpVal != 0) || // if op is "not equal to"
            (op == LT && cmpVal < 0) ||  // if op is "less than"
            (op == LE && cmpVal <= 0) || // if op is "less than or equal to"
            (op == EQ && cmpVal == 0) || // if op is "equal to"
            (op == GT && cmpVal > 0) ||  // if op is "greater than"
            (op == GE && cmpVal >= 0)    // if op is "greater than or equal to"
        )
        {
            /*
            set the search index in the relation cache as
            the record id of the record that satisfies the given condition
            (use RelCacheTable::setSearchIndex function)
            */
            prevRecId = RecId{block, slot};
            RelCacheTable::setSearchIndex(relId, &prevRecId);
            return RecId{block, slot};
        }

        slot++;
    }

    // no record in the relation with Id relid satisfies the given condition
    return RecId{-1, -1};
}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE])
{
    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute newRelationName; // set newRelationName with newName
    strcpy(newRelationName.sVal, newName);

    // search the relation catalog for an entry with "RelName" = newRelationName
    // TODO : Get this dynamically
    char def_RELCAT_ATTR_RELNAME[16] = "RelName";
    RecId recId = linearSearch(RELCAT_RELID, def_RELCAT_ATTR_RELNAME, newRelationName, EQ);

    // If relation with name newName already exists (result of linearSearch
    //                                               is not {-1, -1})
    //    return E_RELEXIST;

    if (recId.block != -1 && recId.slot != -1)
    {
        return E_RELEXIST;
    }

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute oldRelationName; // set oldRelationName with oldName

    strcpy(oldRelationName.sVal, oldName);
    // search the relation catalog for an entry with "RelName" = oldRelationName
    recId = linearSearch(RELCAT_RELID, def_RELCAT_ATTR_RELNAME, oldRelationName, EQ);

    // If relation with name oldName does not exist (result of linearSearch is {-1, -1})
    //    return E_RELNOTEXIST;
    if (recId.block == -1 && recId.slot == -1)
    {
        return E_RELNOTEXIST;
    }

    /* get the relation catalog record of the relation to rename using a RecBuffer
       on the relation catalog [RELCAT_BLOCK] and RecBuffer.getRecord function
    */
    RecBuffer recBuffer(RELCAT_BLOCK);
    HeadInfo head;
    recBuffer.getHeader(&head);
    Attribute record[head.numAttrs];
    recBuffer.getRecord(record, recId.slot);

    /* update the relation name attribute in the record with newName.
       (use RELCAT_REL_NAME_INDEX) */
    // set back the record value using RecBuffer.setRecord
    strcpy(record[RELCAT_REL_NAME_INDEX].sVal, newName);
    recBuffer.setRecord(record, recId.slot);

    /*
    update all the attribute catalog entries in the attribute catalog corresponding
    to the relation with relation name oldName to the relation name newName
    */

    /* reset the searchIndex of the attribute catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    // for i = 0 to numberOfAttributes :
    //     linearSearch on the attribute catalog for relName = oldRelationName
    //     get the record using RecBuffer.getRecord
    //
    //     update the relName field in the record to newName
    //     set back the record using RecBuffer.setRecord
    // TODO : Get this dynamically
    char def_ATTRCAT_ATTR_RELNAME[16] = "RelName";
    int numAttrs = record[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
    for (int i = 0; i < numAttrs; i++)
    {
        recId = linearSearch(ATTRCAT_RELID, def_ATTRCAT_ATTR_RELNAME, oldRelationName, EQ);
        RecBuffer recBuffer(recId.block);
        recBuffer.getRecord(record, recId.slot);
        strcpy(record[ATTRCAT_REL_NAME_INDEX].sVal, newName);
        recBuffer.setRecord(record, recId.slot);
    }

    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE])
{

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr; // set relNameAttr to relName
    strcpy(relNameAttr.sVal, relName);

    // Search for the relation with name relName in relation catalog using linearSearch()
    // If relation with name relName does not exist (search returns {-1,-1})
    //    return E_RELNOTEXIST;
    char def_RELCAT_ATTR_RELNAME[16] = "RelName";
    RecId recId = linearSearch(RELCAT_RELID, def_RELCAT_ATTR_RELNAME, relNameAttr, EQ);

    if (recId.block == -1 && recId.slot == -1)
    {
        return E_RELNOTEXIST;
    }

    /* reset the searchIndex of the attribute catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    /* declare variable attrToRenameRecId used to store the attr-cat recId
    of the attribute to rename */
    RecId attrToRenameRecId{-1, -1};
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

    /* iterate over all Attribute Catalog Entry record corresponding to the
       relation to find the required attribute */
    // TODO : Get this dynamically
    char def_ATTRCAT_ATTR_RELNAME[16] = "RelName";
    while (true)
    {
        // linear search on the attribute catalog for RelName = relNameAttr
        // get the record using RecBuffer.getRecord
        // if the attribute name is oldName, set attrToRenameRecId to block and slot of this record

        RecId recId = linearSearch(ATTRCAT_RELID, def_ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);

        // if there are no more attributes left to check (linearSearch returned {-1,-1})
        //     break;
        if (recId.block == -1 && recId.slot == -1)
        {
            break;
        }

        /* Get the record from the attribute catalog using RecBuffer.getRecord
          into attrCatEntryRecord */
        RecBuffer recBuffer(recId.block);
        recBuffer.getRecord(attrCatEntryRecord, recId.slot);
        printf(" %s - block \n", attrCatEntryRecord[0].sVal);

        // if attrCatEntryRecord.attrName = oldName
        //     attrToRenameRecId = block and slot of this record
        if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0)
        {
            attrToRenameRecId = recId;
            break;
        }

        // if attrCatEntryRecord.attrName = newName
        //     return E_ATTREXIST;
        if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName) == 0)
        {
            return E_ATTREXIST;
        }
    }

    // if attrToRenameRecId == {-1, -1}
    //     return E_ATTRNOTEXIST;
    if (attrToRenameRecId.block == -1 && attrToRenameRecId.slot == -1)
    {
        return E_ATTRNOTEXIST;
    }

    // Update the entry corresponding to the attribute in the Attribute Catalog Relation.
    /*   declare a RecBuffer for attrToRenameRecId.block and get the record at
         attrToRenameRecId.slot */
    //   update the AttrName of the record with newName
    //   set back the record with RecBuffer.setRecord
    RecBuffer recBuffer(attrToRenameRecId.block);
    recBuffer.getRecord(attrCatEntryRecord, attrToRenameRecId.slot);
    strcpy(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);
    recBuffer.setRecord(attrCatEntryRecord, attrToRenameRecId.slot);
    return SUCCESS;
}