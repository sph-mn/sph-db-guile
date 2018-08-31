(library (sph db)
  (export
    db-debug-count-all-btree-entries
    db-debug-display-btree-counts
    db-debug-display-content-left->right
    db-debug-display-content-right->left
    db-delete
    db-exists?
    db-exit
    db-extern-create
    db-extern-data->id
    db-extern-id->data
    db-extern-update
    db-extern?
    db-id-create
    db-id?
    db-identify
    db-index-errors-extern
    db-index-errors-intern
    db-index-errors-relation
    db-index-recreate-extern
    db-index-recreate-intern
    db-index-recreate-relation
    db-init
    db-init-extension
    db-init-extension-add
    db-initialised?
    db-intern-data->id
    db-intern-ensure
    db-intern-id->data
    db-intern-id->string
    db-intern-read
    db-intern-select
    db-intern-small-data->id
    db-intern-small-id->data
    db-intern-small?
    db-intern-update
    db-intern?
    db-node-read
    db-node-select
    db-null
    db-null?
    db-relation-delete
    db-relation-ensure
    db-relation-field-names
    db-relation-read
    db-relation-record-label
    db-relation-record-layout
    db-relation-record-left
    db-relation-record-ordinal
    db-relation-record-right
    db-relation-select
    db-relation-select-fields
    db-relation-select-fields-retrieve
    db-relation-select-read
    db-relation-select-read-fields
    db-relation-select-read-fields-retrieve
    db-relation?
    db-root
    db-selection?
    db-size-octets-data-max
    db-size-octets-data-min
    db-size-octets-id
    db-statistics
    db-status-description
    db-status-module-id->name
    db-txn-abort
    db-txn-active?
    db-txn-call-read
    db-txn-call-write
    db-txn-commit
    db-txn-create-read
    db-txn-create-write
    db-txn?
    db-type-bit-extern
    db-type-bit-id
    db-type-bit-intern
    db-type-bit-intern-small
    db-types
    db-use)
  (import
    (rnrs exceptions)
    (sph)
    (sph exception)
    (sph record)
    (only (guile)
      syntax
      datum->syntax
      syntax->datum
      false-if-exception
      string-index
      symbol-append
      load-extension
      with-throw-handler
      ;used in the shared library
      write
      read)
    (only (rnrs base) set!))

  (define sph-db-description "bindings to use sph-dg databases")
  (load-extension "libguile-sph-dg" "dg_guile_init")
  (define db-relation-field-names (q (left right label ordinal)))
  (define db-relation-record-layout (make-record-layout db-relation-field-names))

  (define-record-accessors db-relation-record-layout (db-relation-record-left (q left))
    (db-relation-record-right (q right)) (db-relation-record-label (q label))
    (db-relation-record-ordinal (q ordinal)))

  (define-syntax-case (db-types name ...) s
    ; prepend "db-type-bit-" to names
    (datum->syntax s
      (pair (q logior)
        (map (l (a) (symbol-append (q db-type-bit-) a)) (syntax->datum (syntax (name ...)))))))

  (define (db-null? a) "any -> boolean" (equal? db-null a))

  (define* (db-relation-select-read txn #:optional left right label retrieve ordinal offset count)
    (db-relation-read (db-relation-select txn left right label retrieve ordinal offset) count))

(define (db-txn-call-read proc)
    "procedure:{db-txn -> any:result} -> any:result
     call proc with a new read transaction.
     the transaction is automatically finished on return with db-txn-abort"
    (let (txn (db-txn-create-read)) (exception-always (db-txn-abort txn) (proc txn))))

  (define (db-txn-call-write proc)
    "procedure:{db-txn -> any:result} -> any:result
     call proc with a new write transaction.
     the transaction is automatically committed on return if not already aborted or committed.
     the transaction is aborted when an unhandled exception occurs"
    (let (txn (db-txn-create-write))
      (exception-intercept-if (proc txn) (db-txn-abort txn)
        (if (db-txn-active? txn) (db-txn-commit txn)))))

  (define-syntax-rules db-use
    ( ( (path (option-name option-value) ...) body ...)
      (begin (db-init path (list (pair (quote option-name) option-value) ...))
        (exception-always (db-exit) (begin body ...))))
    ((path body ...) (db-use (path) body ...)))

  (define (db-init-extension-add . procs) (set! db-init-extension (append db-init-extension procs)))

  (define* (db-intern-id->string txn ids #:optional every?)
    "db-txn (integer ...) [boolean] -> (string ...)" (db-intern-id->data txn ids (q string) every?))

  (define
    (db-relation-select-fields-create-syntax symbol-proc symbol-txn symbol-retrieve symbol-fields .
      values)
    "symbol-fields is a symbol that is composed of letters corresponding to the fields the values are matched on:
     i for id, l for left, r for right. they can be combined like lr.
     symbol-retrieve has the same format as symbol-fields"
    (let (fields (symbol->string symbol-fields))
      (pairs symbol-proc symbol-txn
        (if (string-index fields #\l) (list-ref values 0) (list))
        (if (string-index fields #\r) (list-ref values 1) (list))
        (if (string-index fields #\c) (list-ref values 2) (list))
        (if symbol-retrieve (list symbol-retrieve) (list)))))

  (define-syntax-case (db-relation-select-fields txn pattern-fields value ...) s
    ;a short syntax for db-relation-select.
    ;example: (db-relation-select-fields txn lr (list 1 2) (list 3 4 5))
    (datum->syntax s
      (apply db-relation-select-fields-create-syntax (q db-relation-select)
        (syntax->datum (syntax (txn #f pattern-fields value ...))))))

  (define-syntax-rule (db-relation-select-read-fields txn pattern-fields values ...)
    (db-relation-read (db-relation-select-fields txn pattern-fields values ...)))

  (define-syntax-case
    (db-relation-select-fields-retrieve txn pattern-retrieve pattern-fields value ...) s
    ;example: (db-relation-select-fields txn i lr (list 1 2) (list 3 4 5))
    (datum->syntax s
      (apply db-relation-select-fields-create-syntax (q db-relation-select)
        (syntax->datum (syntax (txn pattern-retrieve pattern-fields value ...))))))

  (define-syntax-rule
    (db-relation-select-read-fields-retrieve txn pattern-retrieve pattern-fields values ...)
    (db-relation-read
      (db-relation-select-fields-retrieve txn pattern-retrieve pattern-fields values ...))))
