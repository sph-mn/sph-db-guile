(library (sph storage dg)
  (export
    dg-debug-count-all-btree-entries
    dg-debug-display-btree-counts
    dg-debug-display-content-left->right
    dg-debug-display-content-right->left
    dg-delete
    dg-exists?
    dg-exit
    dg-extern-create
    dg-extern-data->id
    dg-extern-id->data
    dg-extern-update
    dg-extern?
    dg-id-create
    dg-id?
    dg-identify
    dg-index-errors-extern
    dg-index-errors-intern
    dg-index-errors-relation
    dg-index-recreate-extern
    dg-index-recreate-intern
    dg-index-recreate-relation
    dg-init
    dg-init-extension
    dg-init-extension-add
    dg-initialised?
    dg-intern-data->id
    dg-intern-ensure
    dg-intern-id->data
    dg-intern-id->string
    dg-intern-read
    dg-intern-select
    dg-intern-small-data->id
    dg-intern-small-id->data
    dg-intern-small?
    dg-intern-update
    dg-intern?
    dg-node-read
    dg-node-select
    dg-null
    dg-null?
    dg-relation-delete
    dg-relation-ensure
    dg-relation-field-names
    dg-relation-read
    dg-relation-record-label
    dg-relation-record-layout
    dg-relation-record-left
    dg-relation-record-ordinal
    dg-relation-record-right
    dg-relation-select
    dg-relation-select-fields
    dg-relation-select-fields-retrieve
    dg-relation-select-read
    dg-relation-select-read-fields
    dg-relation-select-read-fields-retrieve
    dg-relation?
    dg-root
    dg-selection?
    dg-size-octets-data-max
    dg-size-octets-data-min
    dg-size-octets-id
    dg-statistics
    dg-status-description
    dg-status-module-id->name
    dg-txn-abort
    dg-txn-active?
    dg-txn-call-read
    dg-txn-call-write
    dg-txn-commit
    dg-txn-create-read
    dg-txn-create-write
    dg-txn?
    dg-type-bit-extern
    dg-type-bit-id
    dg-type-bit-intern
    dg-type-bit-intern-small
    dg-types
    dg-use)
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

  (define sph-storage-dg-description "bindings to use sph-dg databases")
  (load-extension "libguile-sph-dg" "dg_guile_init")
  (define dg-relation-field-names (q (left right label ordinal)))
  (define dg-relation-record-layout (make-record-layout dg-relation-field-names))

  (define-record-accessors dg-relation-record-layout (dg-relation-record-left (q left))
    (dg-relation-record-right (q right)) (dg-relation-record-label (q label))
    (dg-relation-record-ordinal (q ordinal)))

  (define-syntax-case (dg-types name ...) s
    ; prepend "dg-type-bit-" to names
    (datum->syntax s
      (pair (q logior)
        (map (l (a) (symbol-append (q dg-type-bit-) a)) (syntax->datum (syntax (name ...)))))))

  (define (dg-null? a) "any -> boolean" (equal? dg-null a))

  (define* (dg-relation-select-read txn #:optional left right label retrieve ordinal offset count)
    (dg-relation-read (dg-relation-select txn left right label retrieve ordinal offset) count))

  (define (dg-txn-call-read proc)
    "procedure:{dg-txn -> any:result} -> any:result
     call proc with a new read transaction.
     the transaction is automatically finished on return with dg-txn-abort"
    (let (txn (dg-txn-create-read)) (exception-always (dg-txn-abort txn) (proc txn))))

  (define (dg-txn-call-write proc)
    "procedure:{dg-txn -> any:result} -> any:result
     call proc with a new write transaction.
     the transaction is automatically committed on return if not already aborted or committed.
     the transaction is aborted when an unhandled exception occurs"
    (let (txn (dg-txn-create-write))
      (exception-intercept-if (proc txn) (dg-txn-abort txn)
        (if (dg-txn-active? txn) (dg-txn-commit txn)))))

  (define-syntax-rules dg-use
    ( ( (path (option-name option-value) ...) body ...)
      (begin (dg-init path (list (pair (quote option-name) option-value) ...))
        (exception-always (dg-exit) (begin body ...))))
    ((path body ...) (dg-use (path) body ...)))

  (define (dg-init-extension-add . procs) (set! dg-init-extension (append dg-init-extension procs)))

  (define* (dg-intern-id->string txn ids #:optional every?)
    "dg-txn (integer ...) [boolean] -> (string ...)" (dg-intern-id->data txn ids (q string) every?))

  (define
    (dg-relation-select-fields-create-syntax symbol-proc symbol-txn symbol-retrieve symbol-fields .
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

  (define-syntax-case (dg-relation-select-fields txn pattern-fields value ...) s
    ;a short syntax for dg-relation-select.
    ;example: (dg-relation-select-fields txn lr (list 1 2) (list 3 4 5))
    (datum->syntax s
      (apply dg-relation-select-fields-create-syntax (q dg-relation-select)
        (syntax->datum (syntax (txn #f pattern-fields value ...))))))

  (define-syntax-rule (dg-relation-select-read-fields txn pattern-fields values ...)
    (dg-relation-read (dg-relation-select-fields txn pattern-fields values ...)))

  (define-syntax-case
    (dg-relation-select-fields-retrieve txn pattern-retrieve pattern-fields value ...) s
    ;example: (dg-relation-select-fields txn i lr (list 1 2) (list 3 4 5))
    (datum->syntax s
      (apply dg-relation-select-fields-create-syntax (q dg-relation-select)
        (syntax->datum (syntax (txn pattern-retrieve pattern-fields value ...))))))

  (define-syntax-rule
    (dg-relation-select-read-fields-retrieve txn pattern-retrieve pattern-fields values ...)
    (dg-relation-read
      (dg-relation-select-fields-retrieve txn pattern-retrieve pattern-fields values ...))))
