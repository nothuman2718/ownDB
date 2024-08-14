#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <iostream>
#include <stdlib.h>
#include <vector>
#include <string.h>

using namespace std;
int main(int argc, char *argv[])
{
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache;

  /*
  for i = 0 and i = 1 (i.e RELCAT_RELID and ATTRCAT_RELID)

      get the relation catalog entry using RelCacheTable::getRelCatEntry()
      printf("Relation: %s\n", relname);

      for j = 0 to numAttrs of the relation - 1
          get the attribute catalog entry for (rel-id i, attribute offset j)
           in attrCatEntry using AttrCacheTable::getAttrCatEntry()

          printf("  %s: %s\n", attrName, attrType);
  */
  RelCatEntry *relCatBuf = new RelCatEntry();
  RelCacheTable::getRelCatEntry(RELCAT_RELID, relCatBuf);
  cout << "Relation: " << relCatBuf->relName << endl;

  for (int j = 0; j < relCatBuf->numAttrs; j++)
  {
    AttrCatEntry *attrCatBuf = new AttrCatEntry();
    AttrCacheTable::getAttrCatEntry(RELCAT_RELID, j, attrCatBuf);
    cout << "  " << attrCatBuf->attrName << ": " << attrCatBuf->attrType << endl;
  }

  RelCacheTable::getRelCatEntry(ATTRCAT_RELID, relCatBuf);
  cout << "Relation: " << relCatBuf->relName << endl;

  for (int j = 0; j < relCatBuf->numAttrs; j++)
  {
    AttrCatEntry *attrCatBuf = new AttrCatEntry();
    AttrCacheTable::getAttrCatEntry(ATTRCAT_RELID, j, attrCatBuf);
    cout << "  " << attrCatBuf->attrName << ": " << attrCatBuf->attrType << endl;
  }

  RelCacheTable::getRelCatEntry(2, relCatBuf);
  cout << "Relation: " << relCatBuf->relName << endl;

  for (int j = 0; j < relCatBuf->numAttrs; j++)
  {
    AttrCatEntry *attrCatBuf = new AttrCatEntry();
    AttrCacheTable::getAttrCatEntry(2, j, attrCatBuf);
    cout << "  " << attrCatBuf->attrName << ": " << attrCatBuf->attrType << endl;
  }

  return 0;
}