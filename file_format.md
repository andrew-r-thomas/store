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
|   E    |  vNum  | - - page size   |
| (u32) - -       | - - root pid    |
| (u32) - -       |    reserved     |
|PAGES---|--------|--------|--------|
|                                   |
|    three page types:              |
|     - guidepost                   |
|     - leaf                        |
|     - overflow                    |
|                                   |
|--------|--------|--------|--------|
```

```
|GUIDEPOST PAGE---|--------|--------|
|    1   |                          |
|                                   |
|                                   |
|                                   |
|--------|--------|--------|--------|
```

```
|LEAF PAGE--------|--------|--------|
|    0   |  left sibling (u32), 0   |
|for null| right sibling (u32), 0   |
|for null|                          |
|                                   |
|--------|--------|--------|--------|
```

```
|OVERFLOW PAGE----|--------|--------|
|    2   |                          |
|                                   |
|                                   |
|                                   |
|--------|--------|--------|--------|
```


