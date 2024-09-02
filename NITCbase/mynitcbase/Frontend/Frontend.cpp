#include "Frontend.h"
#include <iostream>
#include <cstring>
#include <iostream>

using namespace std;
int Frontend::create_table(char relname[ATTR_SIZE], int no_attrs, char attributes[][ATTR_SIZE], int type_attrs[])
{
  return Schema::createRel(relname, no_attrs, attributes, type_attrs);
}

int Frontend::drop_table(char relname[ATTR_SIZE])
{
  return Schema::deleteRel(relname);
}

int Frontend::open_table(char relname[ATTR_SIZE])
{
  // Schema::openRel
  return Schema::openRel(relname);
}

int Frontend::close_table(char relname[ATTR_SIZE])
{
  // Schema::closeRel
  return Schema::closeRel(relname);
}

int Frontend::alter_table_rename(char relname_from[ATTR_SIZE], char relname_to[ATTR_SIZE])
{
  return Schema::renameRel(relname_from, relname_to);
}

int Frontend::alter_table_rename_column(char relname[ATTR_SIZE], char attrname_from[ATTR_SIZE],
                                        char attrname_to[ATTR_SIZE])
{
  return Schema::renameAttr(relname, attrname_from, attrname_to);
}

int Frontend::create_index(char relname[ATTR_SIZE], char attrname[ATTR_SIZE])
{

  return Schema::createIndex(relname, attrname);
}

int Frontend::drop_index(char relname[ATTR_SIZE], char attrname[ATTR_SIZE])
{
  return Schema::dropIndex(relname, attrname);
}

int Frontend::insert_into_table_values(char relname[ATTR_SIZE], int attr_count, char attr_values[][ATTR_SIZE])
{
  return Algebra::insert(relname, attr_count, attr_values);
}

int Frontend::select_from_table(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE])
{

  return Algebra::project(relname_source, relname_target);
}

int Frontend::select_attrlist_from_table(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE],
                                         int attr_count, char attr_list[][ATTR_SIZE])
{
  // Algebra::project
  return Algebra::project(relname_source, relname_target, attr_count, attr_list);
}

int Frontend::select_from_table_where(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE],
                                      char attribute[ATTR_SIZE], int op, char value[ATTR_SIZE])
{
  // Algebra::select
  return Algebra::select(relname_source, relname_target, attribute, op, value);
}

int Frontend::select_attrlist_from_table_where(
    char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE],
    int attr_count, char attr_list[][ATTR_SIZE],
    char attribute[ATTR_SIZE], int op, char value[ATTR_SIZE])
{
  char TEMPI[] = ".temp";
  // Call select() method of the Algebra Layer to create a temporary target relation
  int ret = Algebra::select(relname_source, TEMPI, attribute, op, value);
  if (ret != SUCCESS)
  {
    return ret;
  }

  // Open the TEMPI relation using OpenRelTable::openRel()
  int tempRelId = OpenRelTable::openRel(TEMPI);
  if (tempRelId < 0 || tempRelId >= MAX_OPEN)
  {
    Schema::deleteRel(TEMPI);
    return tempRelId;
  }

  // Call project() method of the Algebra Layer to create the actual target relation
  ret = Algebra::project(TEMPI, relname_target, attr_count, attr_list);
  if (ret != SUCCESS)
  {
    OpenRelTable::closeRel(tempRelId);
    Schema::deleteRel(TEMPI);
    return ret;
  }

  // Close the TEMPI relation using OpenRelTable::closeRel()
  OpenRelTable::closeRel(tempRelId);

  // Delete the TEMPI relation using Schema::deleteRel()
  Schema::deleteRel(TEMPI);
  // Return any error codes from project() or SUCCESS otherwise
  return SUCCESS;
}

int Frontend::select_from_join_where(char relname_source_one[ATTR_SIZE], char relname_source_two[ATTR_SIZE],
                                     char relname_target[ATTR_SIZE],
                                     char join_attr_one[ATTR_SIZE], char join_attr_two[ATTR_SIZE])
{
  // Algebra::join
  return SUCCESS;
}

int Frontend::select_attrlist_from_join_where(char relname_source_one[ATTR_SIZE], char relname_source_two[ATTR_SIZE],
                                              char relname_target[ATTR_SIZE],
                                              char join_attr_one[ATTR_SIZE], char join_attr_two[ATTR_SIZE],
                                              int attr_count, char attr_list[][ATTR_SIZE])
{
  // Algebra::join + project
  return SUCCESS;
}

int Frontend::custom_function(int argc, char argv[][ATTR_SIZE])
{
  // argc gives the size of the argv array
  // argv stores every token delimited by space and comma

  // implement whatever you desire

  return SUCCESS;
}