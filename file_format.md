# Store file format

**notes**
- may want to consider having root page always be first
- do we want to support multiple schemas?
	- if we do, do we want to support it at the file level, or have one file
      per schema, and have a nice interface in the library for registering files
      and selecting where you want to read/write data
- page ids start at 1 (so that 0 can indicate a null pointer)

```
|HEADER--|--------|--------|--------|
|   S    |   t    |   O    |   r    |
|   E    |  vNum  | - - page size   |
| (u32) - -       | - - root pid    |
| (u32) - -       |                 |
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
|--------|--------|--------|--------|
```

```
|GUIDEPOST PAGE---|--------|--------|
|                                   |
|                                   |
|                                   |
|                                   |
|--------|--------|--------|--------|
```

```
|LEAF PAGE--------|--------|--------|
|                                   |
|                                   |
|                                   |
|                                   |
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

## data types
all numerical types are encoded as big endian

**u8**:
- id: 8 (0x08)
- length: 1
```
|U8------|
|        |
|--------|
```

**u16**
- id: 16 (0x10)
- length: 2
```
|U16-----|--------|
|                 |
|--------|--------|
```

**u32**
- id: 32 (0x20)
- length: 4
```
|U32-----|--------|--------|--------|
|                                   |
|--------|--------|--------|--------|
```

**u64**
- id: 64 (0x40)
- length: 8
```
|U64-----|--------|--------|--------|
|                                   |
|                                   |
|--------|--------|--------|--------|
```

**u128**
- id: 128 (0x80)
- length: 16
```
|U128----|--------|--------|--------|
|                                   |
|                                   |
|                                   |
|                                   |
|--------|--------|--------|--------|
```

**i8**:
- id: 9 (0x09)
- length: 1
```
|I8------|
|        |
|--------|
```

**i16**
- id: 17 (0x11)
- length: 2
```
|I16-----|--------|
|                 |
|--------|--------|
```

**i32**
- id: 33 (0x21)
- length: 4
```
|I32-----|--------|--------|--------|
|                                   |
|--------|--------|--------|--------|
```

**i64**
- id: 65 (0x41)
- length: 8
```
|I64-----|--------|--------|--------|
|                                   |
|                                   |
|--------|--------|--------|--------|
```

**i128**
- id: 129 (0x81)
- length: 16
```
|I128----|--------|--------|--------|
|                                   |
|                                   |
|                                   |
|                                   |
|--------|--------|--------|--------|
```

**f32**
- id: 15 (0x0F)
- length: 4
```
|F32-----|--------|--------|--------|
|                                   |
|--------|--------|--------|--------|
```

**f64**
- id: 13 (0x0D)
- length: 8
```
|F64-----|--------|--------|--------|
|                                   |
|                                   |
|--------|--------|--------|--------|
```

**bool**
bools are all zeros (`00000000`) for false, and `00000001` for true
- id: 11 (0x0B)
- length: 1
```
|BOOL----|
|        |
|--------|
```

**string**
note that chars are represented as single character strings
strings are utf8 encoded bytes, prefixed by a big endian u32 indicating the byte length,
this means the maximum size for a string is 4 gigabytes
- id: 115 (0x73)
- length: 5-4gb
```
|STRING--|--------|--------|--------|
|              length               |
|--------|--------|--------|--------|
|  utf8  |...                       :
:   ^ at least one byte             :
|--------|--------|--------|--------|
```

**blob (byte array)**
the only substantial differences between a blob and a string are the type id,
and the fact that the stored bytes do not need to be utf8 encoded
- id: 187 (0xBB)
- length: 5-4gb
```
|BLOB----|--------|--------|--------|
|              length               |
|--------|--------|--------|--------|
|  byte  |...                       :
:   ^ at least one byte             :
|--------|--------|--------|--------|
```

**unit**
this is the `()` type in Rust. it only has a type id, and zero length
- id: 1 (0x01)
- length: 0

**option**
options have a single byte indicating either a none or some value, and for a
"some" value, the option contains the encoded value after the indicator byte.
options also have a composite type id, with the type of the some value encoded
after the option id.
- id: 79 (0x4F) [followed by some type id]
- length: 1-n
```
|OPTION--|::::::::|::::::::|::::::::|
| 0 or 1 | (if 1) encoded some value|
|--------|::::::::|::::::::|::::::::|
```

**sequence**
for example, a `Vec`. sequences are encoded as a big endian u16 encoding the
length of the sequence, followed by 0 or more encoded values, all of the same
type. This means that sequences can hold at most 65,536 values
sequences also have a composite type id, with the sequence type (the `T` in
`Vec<T>`) encoded after the sequence id
- id: 83 (0x53) [followed by inner type id]
- length: 1-n
```
|SEQ-----|--------|::::::::|::::::::|
|     length      | encoded values  |
|--------|--------|::::::::|::::::::|
```

**tuple**
tuples type id's are encoded as the tuple type id, followed by a big endian
u8 indicating the length of the tuple, followed by that many type ids for each
internal type

tuples themselves are stored simply as ordered lists of their internal items,
since the length and types of a tuple are encoded in the schema, the actual
data does not need any extra encoded information
- id: 84 (0x54)
```
|TYPE_ID-|--------|::::::::|::::::::|
|  0x54  | length |  inner type ids |
|--------|--------|::::::::|::::::::|
```

length: 0-n
```
|TUPLE:::|::::::::|
|   inner values  |
|::::::::|::::::::|
```

**struct**
structs are encoded similarly to tuples, with the addition that the type specifier
includes names for each item in the struct, the names are encoded as utf8 strings,
with a single leading byte indicating the length,
- id: 
```
|TYPE_ID-|--------|::::::::|::::::::|
|  0x54  | length |  inner type ids |
|--------|--------|::::::::|::::::::|

|INNER_ID|--------|::::::::|::::::::|
| encoded  string |  type specifier |
|--------|--------|::::::::|::::::::|
```
length: 0-n
```
|STRUCT::|::::::::|
|   inner values  |
|::::::::|::::::::|
```
