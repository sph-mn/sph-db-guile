(sc-comment
  "generic db-selection type with the option to carry data to be freed when the selection isnt needed anymore.
  db-guile-selection-t is the list element type")

(pre-define
  db-guile-selections-first mi-list-first
  db-guile-selections-rest mi-list-rest
  mi-list-name-prefix db-guile-selections
  mi-list-element-t db-guile-selection-t)

(declare
  db-guile-selection-type-t
  (type
    (enum
      (db-guile-selection-type-relation db-guile-selection-type-record
        db-guile-selection-type-record-index db-guile-selection-type-index)))
  db-guile-selection-t (type (struct (selection void*) (selection-type db-guile-selection-type-t)))
  db-guile-record-selection-t
  (type (struct (matcher SCM) (status-id int) (selection db-record-selection-t)))
  db-guile-relation-selection-t
  (type
    (struct
      (left db-ids-t)
      (right db-ids-t)
      (label db-ids-t)
      (status-id int)
      (scm-from-relations (function-pointer SCM db-relations-t))
      (selection db-relation-selection-t)))
  db-guile-index-selection-t (type (struct (status-id int) (selection db-index-selection-t)))
  db-guile-record-index-selection-t
  (type (struct (status-id int) (selection db-record-index-selection-t))))

(pre-include "./foreign/sph/mi-list.c")
(define db-guile-active-selections (__thread db-guile-selections-t*) 0)

(define (db-guile-selections-free) void
  "finish all selections and associated temporary data of the current thread.
  so that no call to selection-finish is necessary in scheme.
  called by txn-commit or txn-abort.
  there can only be one active transaction per thread per sph-db requirements"
  (declare
    a db-guile-selection-t
    relation-selection db-guile-relation-selection-t
    record-selection db-guile-record-selection-t
    index-selection db-guile-index-selection-t
    record-index-selection db-guile-record-index-selection-t)
  (while db-guile-active-selections
    (set a (db-guile-selections-first db-guile-active-selections))
    (case = a.selection-type
      (db-guile-selection-type-relation
        (set relation-selection
          (pointer-get (convert-type a.selection db-guile-relation-selection-t*)))
        (db-relation-selection-finish &relation-selection.selection)
        (db-ids-free relation-selection.left) (db-ids-free relation-selection.label)
        (db-ids-free relation-selection.right) (free a.selection))
      (db-guile-selection-type-record
        (set record-selection (pointer-get (convert-type a.selection db-guile-record-selection-t*)))
        (db-record-selection-finish &record-selection.selection) (free a.selection))
      (db-guile-selection-type-index
        (set index-selection (pointer-get (convert-type a.selection db-guile-index-selection-t*)))
        (db-index-selection-finish &index-selection.selection) (free a.selection))
      (db-guile-selection-type-record-index
        (set record-index-selection
          (pointer-get (convert-type a.selection db-guile-record-index-selection-t*)))
        (db-record-index-selection-finish &record-index-selection.selection) (free a.selection)))
    (set db-guile-active-selections (db-guile-selections-drop db-guile-active-selections))))

(define (db-guile-selection-register db-selection selection-type)
  (void void* db-guile-selection-type-t)
  "add a new db-guile-selection object to db-guile-active-selections"
  status-declare
  (declare a db-guile-selections-t* b db-guile-selection-t)
  (sc-comment "add db-guile-selection to linked-list")
  (set
    b.selection db-selection
    b.selection-type selection-type
    a (db-guile-selections-add db-guile-active-selections b))
  (if a (set db-guile-active-selections a)
    (begin (status-set db-status-group-db db-status-id-memory) (scm-from-status-error status))))