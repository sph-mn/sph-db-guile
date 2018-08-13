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
clone the code repository or download an archive.

* [github](https://github.com/sph-mn/sph-db-guile/archive/master.zip)

alternatives:
* git: "https://github.com/sph-mn/sph-db-guile"
* git: "git://sph.mn/sph-db-guile"

## setup
* extract eventual archives and change into the project directory
* ./exe/compile-c
* ./exe/install [path-prefix]

# getting started
to load the bindings
```
(import (sph db))
```

the following examples assume that the module ``(sph)`` has also been imported.

## create and use a database

```
(dg-use "/example-path"
  (lambda (env)
  ; commands using the database ...
  ))
```

the database is created if it does not exist.
alternatively there is ``(dg-open "/tmp/example")`` and ``(dg-close env)``.

## store strings, bytevectors or integers (or any "write" serialisable scheme object)

```
(import (rnrs bytevectors))

(dg-txn-call-write
  (l (txn)
    (dg-intern-ensure txn (list "test"))
    (dg-intern-ensure txn (list "test2" -3 (make-bytevector 4 1)))))
```

the result of ``dg-intern-ensure`` are the new element identifiers (integers) in reverse order. when data is already stored then the existing element identifier is returned instead. data is stored typed and types are converted automatically. the same data represented using different types does not resolve to the same id. for example, a bytevector that stores the bytes of an utf-8 string will not be the same as the corresponding string. all serialisable scheme datums can be stored and retrieved. non-serialisable scheme objects like compiled lambdas will not be retrievable as such. for these cases the code to create the objects should be stored as a string instead. internally, some types are stored in native binary formats. strings are always utf-8, should another encoding be required then a bytevector must be used.

there are "implicit transaction" variants for most create/read/delete procedures with names that end with an asterisk in module ``(sph db implicit-txn)``, for example:

```
(dg-intern-ensure* (list "test"))
```

unsigned integers not bigger than dg-size-octets-id can be used cheaply in relations with the type "intern-small".
intern-small nodes are virtual nodes that only exist in relations and are particularly well suited for numbers, for example timestamps.

```
(let*
  ( (node-id (dg-intern-small-data->id 1506209583))
    (timestamp (dg-intern-small-id->data node-id)))
  #t)
```

## create relations

relations are between nodes and the nodes are not checked for existence.
specifying a relation label is optional and the default label is dg-null.
```
(dg-txn-call-write (l (txn)
  (dg-relation-ensure txn (list 1 2 3) (list 4 5))))
```

with a label - labels are also node ids:
```
(dg-txn-call-write (l (txn)
  (dg-relation-ensure txn (list 1 2 3) (list 4 5) (list 6))))
```

## read relations
```
(define left-ids (list 1 2 3))
(define right-ids (list 4 5 6))
(define label-ids (list 7))
(dg-txn-call-write
  (l (txn)
    (let
      ( (selection
          (dg-relation-select txn left-ids right-ids label-ids)))
      (dg-relation-read selection))))
```

gets all relations that match any id of every filter left/right/label-ids ("or"). the empty list matches nothing and leads to an empty result. false disables a filter.
the result is a list of vectors, or node identifiers if retrieve-only-field is not false and a symbol.
the vector element order is left/right/label/ordinal but it is safer to use ``dg-relation-record-left`` and related procedures for access.
there is also ``dg-relation-select-read`` and an implicit-txn variant:
```
(dg-relation-select-read* txn left-ids right-ids label-ids)
```

dg-relation-select has more parameters:
```
dg-relation-select :: txn left right label retrieve-only-field ordinal:((min . integer) (max . integer)) offset
```

dg-relation-delete is similar, but does not need a select:
```
dg-relation-delete :: txn left right label ordinal:((min . integer) (max . integer))
```

## translate internally stored data to identifiers
```
(dg-intern-data->id txn (list "test"))
```

## translate identifiers to internally stored data
```
(dg-intern-id->data txn (list 1 2))
```

## create and write to a file
the "1" stands for the number of new files to create

```
(define files-directory "/tmp/example-path/files/")
(system* "mkdir" "-p" files-directory)
(define (id->path id) (string-append files-directory (number->string id 32)))

(dg-use "/tmp/example-path"
  (let ((identifiers (dg-extern-create* 1)))
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

# notes
* read procedures have initialisation costs. it is slightly more efficient to read more items at once using the "count" parameter
* there can only be one transaction per thread. this matches lmdbs default behaviour
* fast binary live-backups are supported with the mdb_dump, mdb_load or mdb_copy applications from lmdb. dumps are only compatible with the same id and ordinal size and database version

## development
* "dg-guile-generic-read-state-t" have been implemented so that it is possible to free the underlying sph-db reader-states and close cursors before the transaction is finished and the scm object is garbage collected. cursors have to be closed when read transactions are used, but can/should not be closed after the transaction is finished
* dg selections are bound to threads, that is why using the environment option MDB-NOTLS with dg-init probably would not work

# license
gpl3+

# maintainer
http://sph.mn
sph@posteo.eu