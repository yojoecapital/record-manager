# Record Manager

This is a project for my Advanced Database Organization class where I implemented a tiny database-like system from scratch. Each assignment builds upon the code developed in the previous assignment, covering key concepts and data structures discussed in lectures and readings. The assignments include:

1. **Storage Manager**: Implemented a storage manager for reading/writing blocks to/from a file on disk.
2. **Buffer Manager**: Implemented a buffer manager for managing a buffer of blocks in memory, including reading/flushing to disk and block replacement.
3. **Record Manager**: Implemented a simple record manager for navigation through records, and inserting and deleting records.

## Building

To build the record manager and the first set of test cases (`test_assign3_1.c`), use:

```sh
make
./test_assign3_1.o
```

To build the second set of test cases (`test_assign3_2.c`), use:

```sh
make test_assign3_2
./test_assign3_2.o
```

To clean the solution, use:

```sh
make clean
```

**Note:** Cleaning the solution will also remove the default page file `DATA.bin`.

## Explanation of Solution

The first page in the page file is dedicated to defining the system schema/catalog page. The system catalog can be grabbed by casting the `char *data` of the page to an `RM_SystemCatalog`.

### RM_SystemCatalog

```c
typedef struct RM_SystemCatalog {
    int totalNumPages;
    int freePage;
    int numTables;
    RM_SystemSchema tables[MAX_NUM_TABLES];
} RM_SystemCatalog;
```

- **totalNumPages**: The number of pages the file takes up (this value will only grow).
- **freePage**: An index to the first free page or `NO_PAGE` if there are no free pages. Free pages are tracked by a doubly linked list of page pointers.
- **numTables**: The number of tables in the system. The number of tables that can be created is limited by `MAX_NUM_TABLES`.
- **tables**: An array of `RM_SystemSchema` defining the system table schemas saved on the catalog page.

### RM_SystemSchema

```c
typedef struct RM_SystemSchema {
    char name[TABLE_NAME_SIZE];
    int numAttr;
    char attrNames[MAX_NUM_ATTR * ATTR_NAME_SIZE];
    DataType dataTypes[MAX_NUM_ATTR];
    int typeLength[MAX_NUM_ATTR];
    int keySize;
    int keyAttrs[MAX_NUM_KEYS];
    int numTuples;
    int pageNum;
    BM_PageHandle *handle;
} RM_SystemSchema;
```

- **name**: Fixed-size array for table names defined by `TABLE_NAME_SIZE`.
- **numAttr**: Number of attributes.
- **attrNames**: Fixed-size array for attribute names defined by `MAX_NUM_ATTR * ATTR_NAME_SIZE`.
- **dataTypes**: Data types of attributes.
- **typeLength**: Length of each data type.
- **keySize**: Size of the key.
- **keyAttrs**: Attributes that make up the key.
- **numTuples**: Number of tuples in the table.
- **pageNum**: Page number where the table data starts.
- **handle**: Pointer to the page handle if the table is open, `NULL` if closed.

### Page Layout

The page layout of the tables has an `RM_PageHeader` followed by a *slot array* and then the tuple data.

```c
typedef struct RM_PageHeader {
    int nextPage;
    int prevPage;
    int numSlots;
} RM_PageHeader;
```

- **nextPage**: Pointer to the next page.
- **prevPage**: Pointer to the previous page.
- **numSlots**: Number of slots in the page.

The slot array is an array of booleans indicating if the associated space is occupied by a tuple. The number of slots is fixed, allowing the record manager to index into the tuple data after calculating the offset of the header, the slot array size (`numSlots * sizeof(bool)`), and the size of each record (by calling `getRecordSize()`).

## API Functions

### Table and Manager 

```c
RC initRecordManager(void *mgmtData)
```
- Initializes the record manager with the specified page file name or defaults to `DATA.bin`. Sets up the buffer pool and pins the catalog page.

```c
RC shutdownRecordManager()
```
- Unpins the catalog page, shuts down the buffer pool, and ensures no tables are open.

```c
RC createTable(char *name, Schema *schema)
```
- Creates a table with the given name and schema if it doesn't already exist and if the system is not at `MAX_NUM_TABLES`.

```c
RC openTable(RM_TableData *rel, char *name)
```
- Opens the specified table and pins its first page.

```c
RC closeTable(RM_TableData *rel)
```
- Unpins, flushes, and frees the resources of an open table.

```c
RC deleteTable(char *name)
```
- Deletes a closed table by removing its entry from the catalog and appending its pages to the free list.

### Record Handling

```c
RC insertRecord(RM_TableData *rel, Record *record)
```
- Inserts a record into the table, looking for an open slot starting from the main page.

```c
RC deleteRecord(RM_TableData *rel, RID id)
RC updateRecord(RM_TableData *rel, Record *record)
RC getRecord(RM_TableData *rel, RID id, Record *record)
```
- Manipulates records in a table by deleting, updating, or retrieving them based on their `RID`.

### Scans

```c
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
RC next(RM_ScanHandle *scan, Record *record)
RC closeScan(RM_ScanHandle *scan)
```
- Manages table scans by setting up, iterating through, and closing scans based on specified conditions.

### Schema Management

```c
int getRecordSize(Schema *schema)
```
- Calculates the size of a record based on the schema.

```c
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
RC freeSchema(Schema *schema)
```
- Creates and frees schemas for table creation and management.

### Record and Attribute Management

```c
RC createRecord(Record **record, Schema *schema)
RC freeRecord(Record *record)
```
- Manages memory allocation for records.

```c
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
```
- Retrieves an attribute value from a record.

```c
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
```
- Sets an attribute value in a record.

## Buffer Manager

### Hash Table

A hash table is used for the page table, mapping `int -> int` via modulo indexing.

### Additional Definitions

```c
typedef unsigned int TimeStamp;
```
- Timestamp counter for page frames.

```c
typedef struct BM_PageFrame;
```
- Internal struct for page frames containing data pointers, page numbers, fix counts, dirty flags, and timestamps.

```c
typedef struct BM_Metadata;
```
- Internal struct stored in `BM_BufferPool` for managing metadata, including page frames, the page table, the page file handle, a global timestamp, and IO counters.

### Buffer Manager Interface Pool Handling

```c
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
```
- Initializes a buffer pool with metadata and data structures.

```c
RC shutdownBufferPool(BM_BufferPool *const bm)
```
- Shuts down a buffer pool, writing dirty pages to disk and freeing resources.

```c
RC forceFlushPool(BM_BufferPool *const bm)
```
- Writes all dirty and unpinned pages to disk.

### Buffer Manager Interface Access Pages

```c
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
```
- Marks a page as dirty.

```c
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
```
- Unpins a page.

```c
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
```
- Writes a page to disk.

```c
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
```
- Pins a page using FIFO or LRU replacement policy.

### Statistics Interface

```c
PageNumber *getFrameContents(BM_BufferPool *const bm)
bool *getDirtyFlags(BM_BufferPool *const bm)
int *getFixCounts(BM_BufferPool *const bm)
```
- Retrieves statistics on page frames, dirty flags, and fix counts.

```c
int getNumReadIO(BM_BufferPool *const bm)
int getNumWriteIO(BM_BufferPool *const bm)
```
- Retrieves read and write IO counters.

### Replacement Policies

```c
BM_PageFrame *replacementFIFO(BM_BufferPool *const bm)
```
- Implements FIFO replacement policy for page frames.

```c
BM_PageFrame *replacementLRU(BM_BufferPool *const bm)
```
- Implements LRU replacement policy for page frames.

## Storage Manager

### Manipulating Page Files

```c
RC createPageFile(char *fileName)
```

- create a new page file by passing a `fileName`
- write a single page of `PAGE_SIZE` written with `\0` bytes
- this page file does not *stay* open and will be closed in this function. `openPageFile` must be called subsequently to open the page file.

```c
RC openPageFile(char *fileName, SM_FileHandle *fHandle)
```

- open a page file by passing a `fileName` and a pointer to a file handle
- the file handle `fHandle` is populated with
  - `fileName` is set to the passed `fileName`
  - `totalNumPages` is populated by the helper function `_getFileSize` which
    - moves the file pointer to the end of the file
    - uses `ftell` to get the position of the pointer relative to the start of the file (i.e. the file `size`)
    - divide this `size` by `PAGE_SIZE` to get the `totalNumPages`
  - `curPagePos` is set to `0`
  - `mgmtInfo` is used to store the file pointer (the user should not use this as it is used internally by the storage manager)

```c
RC closePageFile (SM_FileHandle *fHandle)
```

- closes the page file using `mgmtInfo` which holds the file pointer
- `mgmtInfo` is then set to `NULL` (i.e. `(void *)0`)

```c
RC destroyPageFile (char *fileName)
```

- removes the page file
- it is safer to call `closePageFile` before calling `destroyPageFile`

### Reading Blocks from Disc

```c
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
```

- read the block indexed at `pageNum` 
  - if its in the range of `0` and `fHandle->totalNumPages`
- stores the page content in `memPage`
- this *does not* change the value of `fHandle->curPagePos`

```c
int getBlockPos (SM_FileHandle *fHandle)
```

- simply returns `fHandle->curPagePos`

```c
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
```

- read the block indexed at `0`
- this *does not* update the value of `fHandle->curPagePos`

```c
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
```

- read the block indexed at `fHandle->curPagePos - 1`
- if it succeeds, `fHandle->curPagePos ` is updated to that value

```c
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
```

- read the block indexed at `fHandle->curPagePos`
- `fHandle->curPagePos ` is *not* updated as it is already set to itself

```c
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
```

- read the block indexed at `fHandle->curPagePos + 1`
- if it succeeds, `fHandle->curPagePos ` is updated to that value

```c
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
```

- read the block indexed at `fHandle->totalNumPages - 1`
- this *does not* update the value of `fHandle->curPagePos`

### Writing Blocks to a Page File

```c
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
```

- write the content of `memPage` to the block indexed at `pageNum` 
  - if its in the range of `0` and `fHandle->totalNumPages`

```c
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
```

- write `memPage` to the block indexed at `fHandle->curPagePos`

```c
RC appendEmptyBlock (SM_FileHandle *fHandle)
```

- move the file pointer to the end of the file and append single page of `PAGE_SIZE` written with `\0` bytes
- `fHandle->totalNumPages` is incremented by 1

```c
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
```

- while `fHandle->totalNumPages` is less than the `numberOfPages` passed, keep calling `appendEmptyBlock`