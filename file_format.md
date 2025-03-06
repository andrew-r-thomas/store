# Store file format

**notes**
- may want to consider having root page always be first
- do we want to support multiple schemas?
	- if we do, do we want to support it at the file level, or have one file
      per schema, and have a nice interface in the library for registering files
      and selecting where you want to read/write data
- page ids start at 1 (so that 0 can indicate a null pointer)

|HEADER--+--------+--------+--------|
|   S    |   t    |   O    |   r    |
|   E    |  vNum  | page size (u16) |
| root pid (u16)  |                 |
|                                   |
|  schema info and reserved (todo)  |
|                                   |
|PAGES---|--------|--------|--------|
|                                   |
|    three page types:              |
|     - guidepost                   |
|     - leaf                        |
|     - overflow                    |
|                                   |
|--------+--------+--------+--------|

|GUIDEPOST PAGE---+--------+--------|
|                                   |
|                                   |
|                                   |
|                                   |
|--------+--------+--------+--------|

|LEAF PAGE--------+--------+--------|
|                                   |
|                                   |
|                                   |
|                                   |
|--------+--------+--------+--------|

|OVERFLOW PAGE----+--------+--------|
|                                   |
|                                   |
|                                   |
|                                   |
|--------+--------+--------+--------|
