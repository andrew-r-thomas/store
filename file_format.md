> THIS IS A DRAFT, AND DOES NOT REPRESENT THE CURRENT STATE OF THE LIBRARY

# Store file format

store keeps keys and values in a single file, each file has a schema

a store file starts with a 16 byte fixed header
```
|HEADER--|--------|--------|--------|
|   'S'  |   'T'  |   'O'  |   'R'  |
|   'E'  | 3 byte semantic version  |
|       page size in bytes (u32)    |
|       root page id (u32)          |
|--------|--------|--------|--------|
```

after the header, there is a variable sized header containing information
about the schema of the keys and values stored in the file, the first schema
block is for the keys, and the second is for the values
```
|SCHEMA HEADER----|--------|--------|
:                                   :
:          key schema block         :--------|
:                                   :        |
:...................................:        |
:                                   :        |
:          val schema block         :--------|
:                                   :        |
|--------|--------|--------|--------|        |
                                             |
|SCHEMA BLOCK-----|--------|--------|        |
|                                   |        |
|               TODO                |<-------|
|                                   |
|--------|--------|--------|--------|
```
when you register a file to a store instance, it will check the schema
in the file against the key and value types you pass to store, and will return
an error if they are not compatable.
> NOTE this does not check that the `Ord` implementation is the same, support
> for this may be added in the future.

after the schema header, a store file is made up entirely of fixed sized pages.
there are three types of pages:
- btree leaf pages
- btree inner pages
- overflow pages

## Btree pages
the header of a btree page is 13 bytes for a leaf page, and 17 bytes for inner
pages. the first byte is an i8 indicating the level of the tree node, a level
of 0 means the page is a leaf node. the next 8 bytes are two u32 page id's for
the left and right sibling of the node respectively. followed by a 2 byte u16
indicating the number of occupied slots in the page, and then another 2 byte u16
indicating the byte offset for the start of the cell content. for inner pages,
the last 4 bytes of the header indicates the right most pointer page id.
```
|BTREE PAGE HEADER|--------|--------|--------|
|  lvl   |          left sib ptr             |
|--------|--------|--------|--------|--------|
|         right sib ptr             |
|--------|--------|--------|--------|
|    num slots    |   cells start   |
|--------|--------|--------|--------|
: right most ptr (only inner pages) :
:........:........:........:........:
```

%%TODO: explain btree page%%

```
|BTREE PAGE-------|--------|--------|
|     slot 1      |     slot 2      |
|     slot 3      |     slot 4      |
:                                   :
|      ...        |     slot n      |
:                                   :
:    some amount of free space      :
:                                   :
|--------|--------|--------|--------|
|                                   |
|              cell 4               |
|                                   |
|--------|--------|--------|--------|
|              cell 3               |
|--------|--------|--------|--------|
|          cell 1          |        |
|--------|--------|--------|        |
|                cell 2             |
|                                   |
|--------|--------|--------|--------|
:                                   :
:                ...                :
:                                   :
|--------|--------|--------|--------|
|              cell n               |
|--------|--------|--------|--------|
```

```
|OVERFLOW PAGE----|--------|--------|
|                                   |
|                                   |
|                                   |
|                                   |
|--------|--------|--------|--------|
```
