#include "Schema.h"
#include <iostream>
#include <cmath>
#include <cstring>

using namespace std;
int Schema::openRel(char relName[ATTR_SIZE])
{
    int ret = OpenRelTable::openRel(relName);

    // the OpenRelTable::openRel() function returns the rel-id if successful
    // a valid rel-id will be within the range 0 <= relId < MAX_OPEN and any
    // error codes will be negative
    if (ret >= 0 && ret <= MAX_OPEN)
    {
        return SUCCESS;
    }

    // otherwise it returns an error message
    return ret;
}

int Schema::closeRel(char relName[ATTR_SIZE])
{
    if (strcmp(relName, RELCAT_RELNAME) == 0 && strcmp(relName, ATTRCAT_RELNAME) == 0)
    {
        return E_NOTPERMITTED;
    }

    // this function returns the rel-id of a relation if it is open or
    // E_RELNOTOPEN if it is not. we will implement this later.
    int relId = OpenRelTable::getRelId(relName);

    if (relId < 0 || relId >= MAX_OPEN)
    {
        return E_RELNOTOPEN;
    }

    return OpenRelTable::closeRel(relId);
}

int Schema::renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE])
{
    // if the oldRelName or newRelName is either Relation Catalog or Attribute Catalog,
    // return E_NOTPERMITTED
    // (check if the relation names are either "RELATIONCAT" and "ATTRIBUTECAT".
    // you may use the following constants: RELCAT_RELNAME and ATTRCAT_RELNAME)
    if (strcmp(oldRelName, RELCAT_RELNAME) == 0 || strcmp(oldRelName, ATTRCAT_RELNAME) == 0 ||
        strcmp(newRelName, RELCAT_RELNAME) == 0 || strcmp(newRelName, ATTRCAT_RELNAME) == 0)
    {
        return E_NOTPERMITTED;
    }

    // if the relation is open
    //    (check if OpenRelTable::getRelId() returns E_RELNOTOPEN)
    //    return E_RELOPEN
    if (OpenRelTable::getRelId(oldRelName) != E_RELNOTOPEN)
    {
        return E_RELOPEN;
    }

    // retVal = BlockAccess::renameRelation(oldRelName, newRelName);
    // return retVal
    return BlockAccess::renameRelation(oldRelName, newRelName);
}

int Schema::renameAttr(char relName[ATTR_SIZE], char oldAttrName[ATTR_SIZE], char newAttrName[ATTR_SIZE])
{
    // if the relName is either Relation Catalog or Attribute Catalog,
    // return E_NOTPERMITTED
    // (check if the relation names are either "RELATIONCAT" and "ATTRIBUTECAT".
    // you may use the following constants: RELCAT_RELNAME and ATTRCAT_RELNAME)
    if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    {
        return E_NOTPERMITTED;
    }

    // if the relation is open
    //    (check if OpenRelTable::getRelId() returns E_RELNOTOPEN)
    //    return E_RELOPEN
    if (OpenRelTable::getRelId(relName) != E_RELNOTOPEN)
    {
        return E_RELOPEN;
    }

    // retVal = BlockAccess::renameAttribute(relName, oldAttrName, newAttrName);
    // return retVal
    return BlockAccess::renameAttribute(relName, oldAttrName, newAttrName);
}