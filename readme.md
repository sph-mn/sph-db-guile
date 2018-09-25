guile scheme bindings for the database [sph-db](https://github.com/sph-mn/sph-db).

status: should work

# dependencies
* run-time
  * [sph-db](https://github.com/sph-mn/sph-db) (gpl3+)
  * guile >= 2
* quick build
  * gcc and shell for the provided compile script
* development build
  * sc - https://github.com/sph-mn/sph-sc (gpl3+)
  * clang-format (part of cmake)

# installation
* install dependencies

## download
clone the code repository or download an archive

* [github](https://github.com/sph-mn/sph-db-guile/archive/master.zip)

alternative sources:
* git: "https://github.com/sph-mn/sph-db-guile"
* git: "git://sph.mn/sph-db-guile"

## setup
* extract eventual archives and change into the project directory
* ./exe/compile-c
* ./exe/install [path-prefix]

installed will be
* a shared library ``{path-prefix}/usr/lib/libguile-sph-db.so``
* scheme modules under ``{path-prefix}/usr/share/guile/site/sph`` and ``{path-prefix}/usr/share/guile/site/test/sph``

# usage
to load the bindings
```
(import (sph db))
```

## select a database
```
(db-use "/tmp/example"
  (lambda (env)
    ; commands using the database ...
  ))
```

the database is created if it does not exist.
alternatively there is ``(db-open "/tmp/example")`` and ``(db-close env)``.

## create a type
```
(define fields (q (("field-1" . int64f) ("field-2" . uint8f) ("field-3" . string8))))
(define type (db-type-create env "test-type" fields))
```

all fixed size types like ``int64f``, types with ``f`` at the end of the name, must come before variable size types like string8.
the names correspond to the field-type-names of sph-db.
currently possible field types are: binary8, binary16, binary32, binary64, string8, string16, string32, string64, string8f, string16f, string32f, string64f, string128f, string256f, binary8f, binary16f, binary32f, binary64f, binary128f, binary256f, uint8f, uint16f, uint32f, uint64f, uint128f, uint256f, int8f, int16f, int32f, int64f, int128f, int256f, float64f

guile apparently doesnt support the ``float`` type (float32f), so data cant be stored in float fields and existing data is read as ``double`` (float64f)

to get a type handle where needed
```
(define type (db-type-get env "test-type"))
```

## create an index
integer field offsets and names supported
```
(define index-fields (list 1 "field-3"))
(define index (db-index-create type index-fields))
```

to get a type handle where needed
```
(db-index-get type index-fields)
```

## create a record
```
(define values (quote ((0 . 123) (1 . 255) (2 . "teststring"))))
(define id (db-record-create txn type values))
```

## read records
### by id
```
(define ids (list 123 2 342 12))
(define records (db-record-get txn ids))
```

db-record-get returns a list of ``db-record`` objects. field values have not been read or converted at this point.
to get record field values
```
(define record (first records))
(define value (db-record-ref type record 1))
(define values (db-record->vector record))
```

a solution for access by field name is work in progress

### by type
```
(define selection (db-record-select txn type))
(define records (db-record-read selection 3))
```
this tries to read the next 3 records. ``db-record-read`` can be called multiple times to read more records. if no more records are found, return an empty list

### by custom matcher procedure
```
(define (matcher type record . custom-state) (pair #t custom-state))
(define selection (db-record-select txn type matcher))
```

custom state values can be provided to the matcher as a single non-list argument or as a list for multiple arguments
```
(define selection (db-record-select txn type matcher (list 1 2 "3")))
```

### via index
only record ids, faster
```
(define index (db-index-get type (list "field-1" "field-3")))
(define selection (db-index-select txn index (q ((0 . 123) (1 . 45)))))
(define ids (db-index-read selection 4))
```

record objects
```
(define selection (db-record-index-select txn index (q ((0 . 123) (1 . 45)))))
(define records (db-record-index-read selection 4))
```

## create relations

relations are between records specified by their ids. no checks are made if valid records exist for the ids.
a relation label is optional, the default is zero

```
(db-txn-call-write (l (txn)
  (db-relation-ensure txn (list 1 2 3) (list 4 5))))
```

with label - labels are also record ids:
```
(db-txn-call-write (l (txn)
  (db-relation-ensure txn (list 1 2 3) (list 4 5) (list 6))))
```

## read relations

```
(define left (list 1 2 3) )
(define right (list 4 5) )
(define label (list 6))
(define selection (db-relation-select txn left right label))
(define relations (db-relation-read selection 100))
```

selects all relations that match any id of every filter left/right/label-ids ("or"). the empty list matches nothing and leads to an empty result. false disables a filter and matches all for that property

``db-relation-read`` returns a list of vectors. the following accessor procedures for relation vectors are available

* db-relation-left
* db-relation-right
* db-relation-label
* db-relation-ordinal

ordinal might be set to false if no filter for a left value was given. this corresponds to the behaviour of sph-db

## virtual records
virtual records are only ids and carry data with the id. the same data and type leads to the same id. they can be used in relations and fields as space saving records
```
(define type (db-type-create env "vtype-uint" (quote (uint16f)) db-type-flag-virtual))
(define value 123)
(define id (db-record-virtual type value))
(equal? value (db-record-virtual-data type id))
```

# db-open options
```
db-open :: path list:options
db-use :: path list:options:((option-name . value) ...) procedure
```

defaults are set by sph-db.

|name|type|description|
| --- | --- | --- |
|file-permissions|integer||
|is-read-only|boolean||
|maximum-reader-count|integer||
|filesystem-has-ordered-writes|boolean||
|env-open-flags|integer|lmdb environment options|

note: db-guile selections are bound to threads, the lmdb option MDB-NOTLS would probably not work

# error handling
rnrs exceptions

# api
```
db-close :: env -> unspecified
db-env-format :: env -> integer:format-id
db-env-maxkeysize :: env -> integer
db-env-open? :: env -> boolean
db-env-root :: env -> string:root-path
db-id-add-type :: integer:id integer:type-id -> integer:id
db-id-element :: integer:id -> integer
db-id-type :: integer:id -> integer:type-id
db-index-create :: env type (field-name-or-offset ...):fields -> index
db-index-delete :: env index -> unspecified
db-index-fields :: index -> list
db-index-get :: env type fields:(integer:offset ...) -> index
db-index-read :: selection integer:count -> (integer:id ...)
db-index-rebuild :: env index -> unspecified
db-index-select :: txn index ((field-offset . any:value) ...) -> selection
db-open :: string:root-path [((key . value) ...):options] -> env
db-record->values :: db-type db-record -> list:((field-offset . value) ...)
db-record->vector :: type record -> vector:#(any:value ...)
db-record-create :: txn type list:((field-offset . value) ...) -> integer:id
db-record-get :: txn list:ids -> (record ...)
db-record-index-read :: selection integer:count -> (record ...)
db-record-index-select :: txn index ((field-offset . any:value) ...) -> selection
db-record-read :: selection integer:count -> (record ...)
db-record-ref :: type record integer:field-offset -> any:value
db-record-select :: txn type [matcher matcher-state] -> selection
db-record-update :: txn type id ((field-offset . value) ...) -> unspecified
db-record-virtual :: type data -> id
db-record-virtual-data :: env id -> any:data
db-relation-ensure :: txn list:left list:right list:label [ordinal-generator ordinal-state] -> unspecified
db-relation-field-names
db-relation-label :: record ->
db-relation-layout
db-relation-left :: record ->
db-relation-ordinal :: record ->
db-relation-read :: selection integer:count -> (vector ...)
db-relation-right :: record ->
db-relation-select :: txn [list:left list:right list:label retrieve ordinal] -> selection
db-statistics :: env -> list
db-txn-abort :: txn -> unspecified
db-txn-active? :: txn -> boolean
db-txn-begin :: env -> db-txn
db-txn-call-read :: db-env procedure:{db-txn -> any:result} -> any:result
db-txn-call-write :: db-env procedure:{db-txn -> any:result} -> any:result
db-txn-commit :: txn -> unspecified
db-txn-write-begin :: env -> db-txn
db-type-create :: env string:name ((string:field-name . symbol:field-type) ...) [integer:flags] -> type
db-type-delete :: env type -> unspecified
db-type-fields :: type -> list
db-type-flag-virtual
db-type-flags :: type -> integer
db-type-get :: env integer/string:id/name -> false/type
db-type-id :: type -> integer:type-id
db-type-indices :: type -> list
db-type-name :: type -> string
db-type-virtual? :: type -> boolean
db-use :: root options c
db-use-p :: string ((key . value) ...) procedure:{db-env -> any} -> any
```

# internals
the main extensions of this binding are:
* free all selections and additionally allocated data automatically when the transaction ends. this is done using a generic selection type and a thread local variable with a linked-list of active selections
* convert from scheme types to field types where appropriate
* create exceptions for status-t errors
* accessors for some structs like env
* big integers are supported

# notes
* there can only be one transaction per thread. this matches lmdbs default behaviour
* fast binary live-backups are supported with the mdb_dump, mdb_load or mdb_copy applications from lmdb. dumps are only compatible with the same database format and version

# license
gpl3+
