guile scheme bindings for the database [sph-db](https://github.com/sph-mn/sph-db).

*wip for sph-db v2018*

the following documentation is completely outdated and for a predecessor of sph-db.

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

## create relations

relations are between records specified by their ids and they are not checked for existence.
a relation label is optional, the default is zero.
```
(db-txn-call-write (l (txn)
  (db-relation-ensure txn (list 1 2 3) (list 4 5))))
```

with label - labels are also record ids:
```
(db-txn-call-write (l (txn)
  (db-relation-ensure txn (list 1 2 3) (list 4 5) (list 6))))
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

note: selections are bound to threads, the lmdb option MDB-NOTLS would probably not work

# internals
the main extensions of this binding are:
* free all selections and additionally allocated data automatically when the transaction ends. this is done using a generic selection type and a thread local variable with a linked-list of active selections
* convert from scheme types to field types where appropriate
* create exceptions for status_t errors
* accessors for some structs like env

# notes
* there can only be one transaction per thread. this matches lmdbs default behaviour
* fast binary live-backups are supported with the mdb_dump, mdb_load or mdb_copy applications from lmdb. dumps are only compatible with the same database format and version

# license
gpl3+

# ---old---

# create type
# create index
# create nodes
# read nodes
# use indices
# virtual records
# error handling

## store strings, bytevectors or integers (or any "write" serialisable scheme object)
the result of ``db-intern-ensure`` are the new element identifiers (integers) in reverse order. when data is already stored then the existing element identifier is returned instead. data is stored typed and types are converted automatically. the same data represented using different types does not resolve to the same id. for example, a bytevector that stores the bytes of an utf-8 string will not be the same as the corresponding string. all serialisable scheme datums can be stored and retrieved. non-serialisable scheme objects like compiled lambdas will not be retrievable as such. for these cases the code to create the objects should be stored as a string instead. internally, some types are stored in native binary formats. strings are always utf-8, should another encoding be required then a bytevector must be used.

unsigned integers not bigger than db-size-octets-id can be used cheaply in relations with the type "intern-small".
intern-small nodes are virtual nodes that only exist in relations and are particularly well suited for numbers, for example timestamps.

```
(let*
  ( (node-id (db-intern-small-data->id 1506209583))
    (timestamp (db-intern-small-id->data node-id)))
  #t)
```

## read relations
```
(define left-ids (list 1 2 3))
(define right-ids (list 4 5 6))
(define label-ids (list 7))
(db-txn-call-write
  (l (txn)
    (let
      ( (selection
          (db-relation-select txn left-ids right-ids label-ids)))
      (db-relation-read selection))))
```

gets all relations that match any id of every filter left/right/label-ids ("or"). the empty list matches nothing and leads to an empty result. false disables a filter.
the result is a list of vectors, or node identifiers if retrieve-only-field is not false and a symbol.
the vector element order is left/right/label/ordinal but it is safer to use ``db-relation-record-left`` and related procedures for access.
there is also ``db-relation-select-read`` and an implicit-txn variant:
```
(db-relation-select-read* txn left-ids right-ids label-ids)
```

db-relation-select has more parameters:
```
db-relation-select :: txn left right label retrieve-only-field ordinal:((min . integer) (max . integer)) offset
```

db-relation-delete is similar, but does not need a select:
```
db-relation-delete :: txn left right label ordinal:((min . integer) (max . integer))
```

## translate internally stored data to identifiers
```
(db-intern-data->id txn (list "test"))
```

## create and write to a file
the "1" stands for the number of new files to create

```
(define files-directory "/tmp/example-path/files/")
(system* "mkdir" "-p" files-directory)
(define (id->path id) (string-append files-directory (number->string id 32)))

(db-use "/tmp/example-path"
  (let ((identifiers (db-extern-create* 1)))
    (call-with-output-file (id->path (first identifiers))
      (lambda (file) (write "testcontent" file)))))
```

the database only tracks the file-name, which in this case is the database element identifier. files can be accessed and modified by any means

## read from a file
if "1" is the identifier for an existing element of type extern, then the following would try to read one scheme expression from a file
```
(call-with-input-file (id->path 1) read)
```

alternative
```
(let ((file (open-file (id->path 1))))
  (read file)
  (close file))
```

# list of exported bindings
module name: ``(sph db)``
```
```
