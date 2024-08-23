#include "OpenRelTable.h"
#include <stdlib.h>
#include <cstring>
#include <iostream>

using namespace std;

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

OpenRelTable::OpenRelTable()
{

    // initialize relCache and attrCache with nullptr
    for (int i = 0; i < MAX_OPEN; ++i)
    {
        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
        tableMetaInfo[i].free = true;
    }

    /************ Setting up Relation Cache entries ************/
    // (we need to populate relation cache with entries for the relation catalog
    //  and attribute catalog.)

    /**** setting up Relation Catalog relation in the Relation Cache Table****/
    RecBuffer relCatBlock(RELCAT_BLOCK);

    Attribute relCatRecord[RELCAT_NO_ATTRS];

    struct RelCacheEntry relCacheEntry;

    for (int i = 0; i < 2; i++)
    {
        relCatBlock.getRecord(relCatRecord, i);
        RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
        relCacheEntry.recId.block = RELCAT_BLOCK;
        relCacheEntry.recId.slot = i;
        RelCacheTable::relCache[i] = (struct RelCacheEntry *)malloc(sizeof(RelCacheEntry));
        *(RelCacheTable::relCache[i]) = relCacheEntry;
    }
    // set the value at RelCacheTable::relCache[ATTRCAT_RELID]

    /************ Setting up Attribute cache entries ************/

    RecBuffer attrCatBlock(ATTRCAT_BLOCK);

    // iterate through all the attributes of the relation catalog and create a linked
    // list of AttrCacheEntry (slots 0 to 5)
    // for each of the entries, set
    //    attrCacheEntry.recId.block = ATTRCAT_BLOCK;
    //    attrCacheEntry.recId.slot = i   (0 to 5)
    //    and attrCacheEntry.next appropriately
    // NOTE: allocate each entry dynamically using malloc
    AttrCacheEntry *head, *curr;

    int k = 0;
    for (int i = 0; i < 2; i++)
    {
        head = curr = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
        int attrCount = RelCacheTable::relCache[i]->relCatEntry.numAttrs;
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        for (int j = 0; j < attrCount - 1; j++)
        {
            attrCatBlock.getRecord(attrCatRecord, k);
            AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &curr->attrCatEntry);
            curr->recId.slot = k;
            curr->recId.block = ATTRCAT_BLOCK;
            curr->next = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
            curr = curr->next;
            k++;
        }
        attrCatBlock.getRecord(attrCatRecord, k);
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &curr->attrCatEntry);
        curr->next = nullptr;
        curr->recId.slot = k;
        curr->recId.block = ATTRCAT_BLOCK;
        k++;
        AttrCacheTable::attrCache[i] = head;
    }

    /************ Setting up tableMetaInfo entries ************/

    tableMetaInfo[RELCAT_RELID].free = false;
    strcpy(tableMetaInfo[RELCAT_RELID].relName, RELCAT_RELNAME);
    tableMetaInfo[ATTRCAT_RELID].free = false;
    strcpy(tableMetaInfo[ATTRCAT_RELID].relName, ATTRCAT_RELNAME);
}

int OpenRelTable::getFreeOpenRelTableEntry()
{

    /* traverse through the tableMetaInfo array,
      find a free entry in the Open Relation Table.*/
    for (int i = 0; i < MAX_OPEN; i++)
    {
        if (tableMetaInfo[i].free)
            return i;
    }

    return E_CACHEFULL;
    // if found return the relation id, else return E_CACHEFULL.
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE])
{
    for (int i = 0; i < MAX_OPEN; i++)
    {
        if (tableMetaInfo[i].free)
            continue;
        if (strcmp(tableMetaInfo[i].relName, relName) == 0)
            return i;
    }

    return E_RELNOTOPEN;
}

int OpenRelTable::openRel(char relName[ATTR_SIZE])
{
    int ret = OpenRelTable::getRelId(relName);
    if (ret != E_RELNOTOPEN)
    {
        return ret;
    }

    /* find a free slot in the Open Relation Table
       using OpenRelTable::getFreeOpenRelTableEntry(). */
    ret = OpenRelTable::getFreeOpenRelTableEntry();
    if (ret == E_CACHEFULL)
    {
        return E_CACHEFULL;
    }

    // let relId be used to store the free slot.
    int relId = ret;

    /****** Setting up Relation Cache entry for the relation ******/

    /* search for the entry with relation name, relName, in the Relation Catalog using
        BlockAccess::linearSearch().
        Care should be taken to reset the searchIndex of the relation RELCAT_RELID
        before calling linearSearch().*/
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    // relcatRecId stores the rec-id of the relation `relName` in the Relation Catalog.
    Attribute check_relname;
    strcpy(check_relname.sVal, relName);
    // TODO's: Get this dynamically from RELCAT's ATTRCAT
    char def_RELCAT_ATTR_RELNAME[16] = "RelName";
    RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID, def_RELCAT_ATTR_RELNAME, check_relname, EQ);

    if (relcatRecId.block == -1 && relcatRecId.slot == -1)
    {
        // (the relation is not found in the Relation Catalog.)
        return E_RELNOTEXIST;
    }

    /* read the record entry corresponding to relcatRecId and create a relCacheEntry
        on it using RecBuffer::getRecord() and RelCacheTable::recordToRelCatEntry().
        update the recId field of this Relation Cache entry to relcatRecId.
        use the Relation Cache entry to set the relId-th entry of the RelCacheTable.
      NOTE: make sure to allocate memory for the RelCacheEntry using malloc()
    */
    RecBuffer recEntry(relcatRecId.block);
    struct RelCacheEntry recCacheEntry;
    Attribute attrRecord[RELCAT_NO_ATTRS];
    recEntry.getRecord(attrRecord, relcatRecId.slot);
    RelCacheTable::recordToRelCatEntry(attrRecord, &recCacheEntry.relCatEntry);
    recCacheEntry.recId = relcatRecId;

    RelCacheTable::relCache[relId] = (struct RelCacheEntry *)malloc(sizeof(struct RelCacheEntry));
    *(RelCacheTable::relCache[relId]) = recCacheEntry;

    /****** Setting up Attribute Cache entry for the relation ******/
    RecBuffer attrEntry(relcatRecId.block);
    // let listHead be used to hold the head of the linked list of attrCache entries.
    AttrCacheEntry *listHead, *curr;
    listHead = curr = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
    int totalAttrs = recCacheEntry.relCatEntry.numAttrs;
    for (int i = 0; i < totalAttrs - 1; i++)
    {
        curr->next = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
        curr = curr->next;
    }
    curr->next = nullptr;
    curr = listHead;
    /*iterate over all the entries in the Attribute Catalog corresponding to each
    attribute of the relation relName by multiple calls of BlockAccess::linearSearch()
    care should be taken to reset the searchIndex of the relation, ATTRCAT_RELID,
    corresponding to Attribute Catalog before the first call to linearSearch().*/

    // TODO's: Get this dynamically from ATTRCAT's ATTRCAT
    char def_ATTRCAT_ATTR_RELNAME[16] = "RelName";
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    for (int i = 0; i < totalAttrs; i++)
    {
        /* let attrcatRecId store a valid record id an entry of the relation, relName,
        in the Attribute Catalog.*/
        Attribute check_relname;
        strcpy(check_relname.sVal, relName);
        RecId attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, def_ATTRCAT_ATTR_RELNAME, check_relname, EQ);

        /* read the record entry corresponding to attrcatRecId and create an
        Attribute Cache entry on it using RecBuffer::getRecord() and
        AttrCacheTable::recordToAttrCatEntry().
        update the recId field of this Attribute Cache entry to attrcatRecId.
        add the Attribute Cache entry to the linked list of listHead .*/
        // NOTE: make sure to allocate memory for the AttrCacheEntry using malloc()
        RecBuffer recbuffer(attrcatRecId.block);
        Attribute record[ATTRCAT_NO_ATTRS];
        recbuffer.getRecord(record, attrcatRecId.slot);
        AttrCacheTable::recordToAttrCatEntry(record, &curr->attrCatEntry);
        curr->recId = attrcatRecId;
        curr = curr->next;
    }

    // set the relIdth entry of the AttrCacheTable to listHead.
    AttrCacheTable::attrCache[relId] = listHead;
    /****** Setting up metadata in the Open Relation Table for the relation******/

    // update the relIdth entry of the tableMetaInfo with free as false and
    // relName as the input.
    tableMetaInfo[relId].free = false;
    strcpy(tableMetaInfo[relId].relName, relName);

    return relId;
}

int OpenRelTable::closeRel(int relId)
{
    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
    {
        return E_NOTPERMITTED;
    }

    if (relId < 0 || relId >= MAX_OPEN)
    {
        // cout << relId << endl;
        return E_OUTOFBOUND;
    }

    if (tableMetaInfo[relId].free)
    {
        return E_RELNOTOPEN;
    }

    // free the memory allocated in the relation and attribute caches which was
    // allocated in the OpenRelTable::openRel() function
    if (RelCacheTable::relCache[relId] != nullptr)
    {
        if (RelCacheTable::relCache[relId]->dirty)
        {

            /* Get the Relation Catalog entry from RelCacheTable::relCache
            Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
            RelCatEntry relCatEntry = RelCacheTable::relCache[relId]->relCatEntry;
            union Attribute record[RELCAT_NO_ATTRS];
            RelCacheTable::relCatEntryToRecord(&relCatEntry, record);

            // declaring an object of RecBuffer class to write back to the buffer
            RecBuffer relCatBlock(RelCacheTable::relCache[relId]->recId.block);

            // Write back to the buffer using relCatBlock.setRecord() with recId.slot
            relCatBlock.setRecord(record, RelCacheTable::relCache[relId]->recId.slot);
        }
        free(RelCacheTable::relCache[relId]);
    }

    // we will do write back in subsequent stages
    AttrCacheEntry *head = AttrCacheTable::attrCache[relId];
    AttrCacheEntry *temp = nullptr;
    while (head != nullptr)
    {
        temp = head;
        head = head->next;
        free(temp);
        temp = nullptr;
    }
    RelCacheTable::relCache[relId] = nullptr;
    AttrCacheTable::attrCache[relId] = nullptr;

    // update `tableMetaInfo` to set `relId` as a free slot
    tableMetaInfo[relId].free = true;
    tableMetaInfo[relId].relName[0] = '\0';

    return SUCCESS;
}

OpenRelTable::~OpenRelTable()
{
    for (int i = 2; i < MAX_OPEN; ++i)
    {
        if (tableMetaInfo[i].free == false)
        {
            OpenRelTable::closeRel(i);
        }
    }

    // free all the memory that you allocated in the constructor
    for (int i = 0; i <= 1; i++)
    {
        if (RelCacheTable::relCache[i] != nullptr)
        {
            free(RelCacheTable::relCache[i]);
            RelCacheTable::relCache[i] = nullptr;
        }
    }

    for (int i = 0; i <= 1; i++)
    {
        // free attrcache
        AttrCacheEntry *entry = AttrCacheTable::attrCache[i];
        while (entry)
        {
            AttrCacheEntry *temp = entry;
            entry = entry->next;
            free(temp);
            temp = nullptr;
        }
        AttrCacheTable::attrCache[i] = nullptr;
    }
}