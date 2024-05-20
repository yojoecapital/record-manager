#include <stdlib.h>
#include "dberror.h"
#include "expr.h"
#include "record_mgr.h"
#include "test_helper.h"
#include <string.h>
#include <stdio.h>

#define TABLE_NAME "table"
#define TABLE_NAME_2 "students"
#define TABLE_NAME_3 "fruits"
#define TABLE_NAME_4 "departments"
#define PAGE_FILE_NAME "DATA.bin"
#define MAX_TEST_LENGTH 8

void testTableCreation();
void testTableDeletion();
void testRecords();
void testManyRecords();
char* prepend_helper_int(int i, char c, char result[MAX_TEST_LENGTH]);
char* prepend_helper_string(char *s, char c, char result[MAX_TEST_LENGTH]);

char* prepend_helper_int(int i, char c, char result[MAX_TEST_LENGTH]) {
    snprintf(result, MAX_TEST_LENGTH, "%c%d", c, i);
    return result;
}

char* prepend_helper_string(char *s, char c, char result[MAX_TEST_LENGTH]) {
    snprintf(result, MAX_TEST_LENGTH, "%c%s", c, s);
    return result;
}

int main () 
{
    testTableCreation();
    testTableDeletion();
    testRecords();
    testManyRecords();
    return 0;
}

void testTableCreation()
{
    char* testName = "testTableCreation";
    remove(PAGE_FILE_NAME);

    // open the RM & make a table
    TEST_CHECK(initRecordManager(NULL));
    int numAttr = 3;
    char *attrNames[] = { "a", "b", "c" };
    DataType dataTypes[] = { DT_INT, DT_INT, DT_INT };
    int typeLengths[] = { 0, 0, 0 };
    int keySize = 1;
    int keys[] = { 0 };
    Schema *schema = createSchema(numAttr, attrNames, dataTypes, typeLengths, keySize, keys);
    ASSERT_EQUALS_INT((int)(sizeof(int) * numAttr), getRecordSize(schema), "size of record should be 3 ints");
    RM_TableData rel;
    TEST_CHECK(createTable(TABLE_NAME, schema));
    TEST_CHECK(openTable(&rel, TABLE_NAME));
    TEST_CHECK(closeTable(&rel));
    TEST_CHECK(freeSchema(schema));

    TEST_CHECK(shutdownRecordManager());

    // open the RM again & check if the table schema matches
    TEST_CHECK(initRecordManager(NULL));
    TEST_CHECK(openTable(&rel, TABLE_NAME));

    if (rel.schema->numAttr != numAttr || rel.schema->keySize != keySize)
    {
        printf("bad table (numAttr, keySize)\n");
        return;
    }

    for (int attrIndex = 0; attrIndex < numAttr; attrIndex++)
    {
        if (rel.schema->dataTypes[attrIndex] != dataTypes[attrIndex] || rel.schema->typeLength[attrIndex] != typeLengths[attrIndex])
        {
            printf("bad table (dataTypes, typeLength)\n");
            return;
        }
        if (strcmp(rel.schema->attrNames[attrIndex], attrNames[attrIndex]) != 0)
        {
            printf("bad table (attrNames)\n");
            return;
        }
    }

    for (int keyIndex = 0; keyIndex < keySize; keyIndex++)
    {
        if (rel.schema->keyAttrs[keyIndex] != keys[keyIndex])
        {
            printf("bad table (keyAttrs)\n");
            return;
        }
    }

    ASSERT_EQUALS_INT(1, getNumTables(), "should be 1 table");
    TEST_CHECK(closeTable(&rel));
    TEST_CHECK(shutdownRecordManager());
    //remove(PAGE_FILE_NAME);
    TEST_DONE();
}

void testTableDeletion()
{
    char* testName = "testTableCreation";
    remove(PAGE_FILE_NAME);

    // open the RM & make a table
    TEST_CHECK(initRecordManager(NULL));
    int numAttr = 3;
    char *attrNames[] = { "a", "b", "c" };
    DataType dataTypes[] = { DT_INT, DT_INT, DT_INT };
    int typeLengths[] = { 0, 0, 0 };
    int keySize = 1;
    int keys[] = { 0 };
    Schema *schema = createSchema(numAttr, attrNames, dataTypes, typeLengths, keySize, keys);
    TEST_CHECK(createTable(TABLE_NAME, schema));
    TEST_CHECK(createTable(TABLE_NAME_2, schema));
    TEST_CHECK(createTable(TABLE_NAME_3, schema));
    TEST_CHECK(createTable(TABLE_NAME_4, schema));
    ASSERT_EQUALS_INT(4, getNumTables(), "should be 4 tables after 4 creates");
    TEST_CHECK(shutdownRecordManager());

    // open the RM & delete the table
    TEST_CHECK(initRecordManager(NULL));
    TEST_CHECK(deleteTable(TABLE_NAME));
    TEST_CHECK(deleteTable(TABLE_NAME_3));
    ASSERT_EQUALS_INT(2, getNumTables(), "should be 2 tables after 2 deletes");
    ASSERT_EQUALS_INT(2, getNumFreePages(), "should be 2 free pages");
    TEST_CHECK(shutdownRecordManager());

    // open the RM & make the same table
    TEST_CHECK(initRecordManager(NULL));
    TEST_CHECK(createTable(TABLE_NAME, schema));
    TEST_CHECK(createTable(TABLE_NAME_3, schema));
    ASSERT_EQUALS_INT(4, getNumTables(), "should be 4 table after 2 creates");
    ASSERT_EQUALS_INT(0, getNumFreePages(), "should be 0 free pages");
    ASSERT_EQUALS_INT(5, getNumPages(), "should be 5 total pages");
    TEST_CHECK(freeSchema(schema));
    TEST_CHECK(shutdownRecordManager());

    TEST_DONE();
}

void testRecords()
{
    char* testName = "testRecords";
    remove(PAGE_FILE_NAME);

    // open the RM & insert a record
    TEST_CHECK(initRecordManager(NULL));
    int numAttr = 4;
    char *attrNames[] = { "ayat", "surahs", "pages", "book" };
    DataType dataTypes[] = { DT_INT, DT_INT, DT_INT, DT_STRING };
    int typeLengths[] = { 0, 0, 0, 8 };
    int keySize = 1;
    int keys[] = { 0 };
    Schema *schema = createSchema(numAttr, attrNames, dataTypes, typeLengths, keySize, keys);
    TEST_CHECK(createTable(TABLE_NAME, schema));
    TEST_CHECK(freeSchema(schema));
    Record *record;
    RM_TableData rel;
    TEST_CHECK(openTable(&rel, TABLE_NAME));
    TEST_CHECK(createRecord(&record, rel.schema));
    Value *ayat = stringToValue("i6326");
    Value *surahs = stringToValue("i114");
    Value *pages = stringToValue("i604");
    Value *book = stringToValue("sQuran");
    Value *value;
    setAttr(record, rel.schema, 0, ayat);
    setAttr(record, rel.schema, 1, surahs);
    setAttr(record, rel.schema, 2, pages);
    setAttr(record, rel.schema, 3, book);
    TEST_CHECK(getAttr(record, rel.schema, 3, &value));
    ASSERT_EQUALS_STRING("Quran", value->v.stringV, "book should be \"Quran\"");
    TEST_CHECK(getAttr(record, rel.schema, 2, &value));
    ASSERT_EQUALS_INT(604, value->v.intV, "pages should be 604");
    TEST_CHECK(insertRecord(&rel, record));
    ASSERT_EQUALS_INT(record->id.page, 1, "should be page 1");
    ASSERT_EQUALS_INT(record->id.slot, 0, "should be slot 0");
    freeVal(ayat);
    freeVal(surahs);
    freeVal(pages);
    freeVal(book);
    TEST_CHECK(closeTable(&rel));
    TEST_CHECK(shutdownRecordManager());

    // open the RM & check if the record is still there
    TEST_CHECK(initRecordManager(NULL));
    TEST_CHECK(openTable(&rel, TABLE_NAME));
    RID id;
    id.page = 1;
    id.slot = 0;
    TEST_CHECK(getRecord(&rel, id, record));
    TEST_CHECK(getAttr(record, rel.schema, 3, &value));
    ASSERT_EQUALS_STRING("Quran", value->v.stringV, "book should still be \"Quran\"");
    TEST_CHECK(getAttr(record, rel.schema, 2, &value));
    ASSERT_EQUALS_INT(604, value->v.intV, "pages should still be 604");

    // delete the record
    TEST_CHECK(deleteRecord(&rel, id));
    ASSERT_ERROR(getRecord(&rel, id, record), "opening a deleted record should fail");
    TEST_CHECK(freeRecord(record));
    TEST_CHECK(closeTable(&rel));
    TEST_CHECK(shutdownRecordManager());
    TEST_DONE();
}

// struct for test records
typedef struct TestRecord {
	int a;
	char *b;
	int c;
} TestRecord;



void testManyRecords()
{
    int N = 10000;
    RID rids[N];

    char* testName = "testManyRecords";
    remove(PAGE_FILE_NAME);

    // open the RM & insert a record
    TEST_CHECK(initRecordManager(NULL));
    int numAttr = 3;
    char *attrNames[] = { "a", "b", "c" };
    DataType dataTypes[] = { DT_INT, DT_STRING, DT_INT };
    int typeLengths[] = { 0, 4, 0 };
    int keySize = 1;
    int keys[] = { 0 };
    Schema *schema = createSchema(numAttr, attrNames, dataTypes, typeLengths, keySize, keys);
    TEST_CHECK(createTable(TABLE_NAME, schema));
    TEST_CHECK(freeSchema(schema));
    Record *record;
    RM_TableData rel;
    TEST_CHECK(openTable(&rel, TABLE_NAME));
    TEST_CHECK(createRecord(&record, rel.schema));

    TestRecord inserts[] = {
			{1, "aaaa", 0},
			{2, "bbbb", 1},
			{3, "cccc", 2},
			{4, "dddd", 3},
			{5, "eeee", 4},
			{6, "ffff", 5},
			{7, "gggg", 6},
			{8, "hhhh", 7},
			{9, "iiii", 8},
			{10, "jjjj", 9},
	};

    // set attributes
    for(int i = 0; i < N; i++)
    {
        TestRecord new = inserts[i % 10];
        char resultA[MAX_TEST_LENGTH];
        char resultB[MAX_TEST_LENGTH];
        char resultC[MAX_TEST_LENGTH];
        Value *a = stringToValue(prepend_helper_int(new.a, 'i', resultA));
        Value *b = stringToValue(prepend_helper_string(new.b, 's', resultB));
        Value *c = stringToValue(prepend_helper_int(new.c, 'i', resultC));
        setAttr(record, rel.schema, 0, a);
        setAttr(record, rel.schema, 1, b);
        setAttr(record, rel.schema, 2, c);
        TEST_CHECK(insertRecord(&rel, record));
        rids[i] = record->id;
    }

    TEST_CHECK(closeTable(&rel));
    TEST_CHECK(shutdownRecordManager());
    TEST_CHECK(initRecordManager(NULL));
    TEST_CHECK(openTable(&rel, TABLE_NAME));

    // check attributes
    for(int i = 0; i < N; i++)
    {
        Value *value;
        TestRecord check = inserts[i % 10];
        TEST_CHECK(getRecord(&rel, rids[i], record));
        TEST_CHECK(getAttr(record, rel.schema, 0, &value));
        if (check.a != value->v.intV)
        {
            printf("mismatch on column a [%d %d], should be %d, is %d\n", rids[i].page, rids[i].slot, check.a, value->v.intV);
        }
        ASSERT_EQUALS_INT(check.a, value->v.intV, "column a should match");
        TEST_CHECK(getAttr(record, rel.schema, 1, &value));
        if (strcmp(check.b, value->v.stringV))
        {
            printf("mismatch on column b [%d %d], should be %s, is %s\n", rids[i].page, rids[i].slot, check.b, value->v.stringV);
        }
        ASSERT_EQUALS_STRING(check.b, value->v.stringV, "column b should match");
        TEST_CHECK(getAttr(record, rel.schema, 2, &value));
        if (check.c != value->v.intV)
        {
            printf("mismatch on column c [%d %d], should be %d, is %d\n", rids[i].page, rids[i].slot, check.c, value->v.intV);
        }
        ASSERT_EQUALS_INT(check.c, value->v.intV, "column c should match");
        freeVal(value);
    }

    freeRecord(record);
    TEST_CHECK(closeTable(&rel));
    TEST_CHECK(shutdownRecordManager());

    TEST_DONE();
}