# Store file format

> OK ACTUALLY WE'RE GONNA JUST DO BYTE STRINGS FOR KEYS & VALUES, NO SCHEMA

**notes**
- may want to consider having root page always be first
- page ids start at 1 (so that 0 can indicate a null pointer)
- no schemas, keys and values are arbitrary byte strings, keys and values are
encoded as follows

```
|KEYorVAl|--------|--------|--------|
|              length               |
:                                   :
:            some bytes             :
:                                   :
|--------|--------|--------|--------|
```

```
|HEADER--|--------|--------|--------|
|   S    |   t    |   O    |   r    |
|   E    |    3 byte semver         |
|           page size (u32)         |
|           root pid (u32)          |
|PAGES---|--------|--------|--------|
|                                   |
|    three page types:              |
|     - inner                       |
|     - leaf                        |
|     - overflow                    |
|                                   |
|--------|--------|--------|--------|
```

```
|INNER PAGE-------|--------|--------|
|lvl (i8)|                          |
|                                   |
|                                   |
|                                   |
|--------|--------|--------|--------|
```

```
|LEAF PAGE--------|--------|--------|--------|--------|
|HEADER--|--------|--------|--------|--------|--------|
|    0   |  left sibling (u32), 0 for null   |
|for null| right sibling (u32), 0 - |
|for null| num slots (u16) | slot - |
|content |
```

```
|OVERFLOW PAGE----|--------|--------|
|   -1   |                          |
|                                   |
|                                   |
|                                   |
|--------|--------|--------|--------|
```


