(sc-comment
  "generic db-selection type with the option to carry data to be freed when the selection isnt needed anymore")

(pre-define
  db-guile-selection-first mi-list-first
  db-guile-selection-rest mi-list-rest
  mi-list-name-prefix db-guile-selections
  mi-list-element-t db-guile-selection-t)

(declare
  db-guile-selection-type-t
  (type (enum (db-guile-selection-type-relation db-guile-selection-type-record)))
  db-guile-selection-t
  (type
    (struct
      (selection void*)
      (selection-type db-guile-selection-type-t)))
  db-guile-relation-selection-t
  (type
    (struct
      (left db-ids-t)
      (right db-ids-t)
      (label db-ids-t)
      (relations->scm (function-pointer SCM db-relations-t))
      (selection db-relation-selection-t))))

(pre-include "./foreign/sph/mi-list.c")
(define db-guile-active-selections (__thread db-guile-selections-t*) 0)

(define (db-guile-selections-free) void
  "finalise all selections of the current thread for garbage collection.
  there can only be one transaction per thread per sph-db requirements"
  (declare
    a db-guile-selection-t
    relation-selection db-guile-relation-selection-t
    record-selection db-record-selection-t*)
  (while db-guile-active-selections
    (set a (db-guile-selections-first db-guile-active-selections))
    (case = a.selection-type
      (db-guile-selection-type-relation
        (set relation-selection
          (pointer-get (convert-type a.selection db-guile-relation-selection-t*)))
        (db-relation-selection-finish &relation-selection.selection)
        (db-ids-free relation-selection.left)
        (db-ids-free relation-selection.label)
        (db-ids-free relation-selection.right) (free a.selection))
      (db-guile-selection-type-record
        (set record-selection a.selection)
        (db-record-selection-finish a.selection) (free a.selection)))
    (set db-guile-active-selections (db-guile-selections-drop db-guile-active-selections))))

(define (db-guile-selection-register db-selection selection-type)
  (void void* db-guile-selection-type-t)
  "add a new db-guile-selection object to db-guile-active-selections"
  (declare
    a db-guile-selections-t*
    b db-guile-selection-t)
  (sc-comment "add db-guile-selection to linked-list")
  (set
    b.data db-selection
    b.selection-type selection-type
    a (db-guile-selections-add db-guile-active-selections b))
  (if a (set db-guile-active-selections a)
    (begin
      (status-set-both db-status-group-db db-status-id-memory)
      (status->scm-error status))))