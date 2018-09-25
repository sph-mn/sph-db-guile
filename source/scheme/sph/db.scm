(library (sph db)
  (export
    db-close
    db-env-format
    db-env-maxkeysize
    db-env-open?
    db-env-root
    db-id-add-type
    db-id-element
    db-id-type
    db-index-create
    db-index-delete
    db-index-fields
    db-index-get
    db-index-read
    db-index-rebuild
    db-index-select
    db-open
    db-record->values
    db-record->vector
    db-record-create
    db-record-get
    db-record-index-read
    db-record-index-select
    db-record-read
    db-record-ref
    db-record-select
    db-record-update
    db-record-virtual
    db-record-virtual-data
    db-record.index-read
    db-relation-ensure
    db-relation-field-names
    db-relation-label
    db-relation-layout
    db-relation-left
    db-relation-ordinal
    db-relation-read
    db-relation-right
    db-relation-select
    db-selection?
    db-statistics
    db-status-description
    db-status-group-id->name
    db-txn-abort
    db-txn-active?
    db-txn-begin
    db-txn-call-read
    db-txn-call-write
    db-txn-commit
    db-txn-write-begin
    db-type-create
    db-type-delete
    db-type-fields
    db-type-flag-virtual
    db-type-flags
    db-type-get
    db-type-id
    db-type-indices
    db-type-name
    db-type-virtual?
    db-use
    db-use-p)
  (import
    (rnrs exceptions)
    (sph)
    (sph exception)
    (sph record)
    (sph vector)
    (only (guile)
      syntax
      datum->syntax
      syntax->datum
      false-if-exception
      string-index
      symbol-append
      load-extension
      with-throw-handler))

  (define sph-db-description
    "bindings to use sph-db.
     # syntax
     db-use
       :: string:root ((option-name value) ...) procedure -> any
       :: string:root procedure -> any")

  (load-extension "libguile-sph-db" "db_guile_init")
  (define db-relation-field-names (q (left right label ordinal)))
  (define db-relation-layout (make-record-layout db-relation-field-names))

  (define-record-accessors db-relation-layout (db-relation-left (q left))
    (db-relation-right (q right)) (db-relation-label (q label)) (db-relation-ordinal (q ordinal)))

  (define (db-use-p root options c) "string ((key . value) ...) procedure:{db-env -> any} -> any"
    (let (env (db-open root options)) (exception-always (db-close env) (c env))))

  (define-syntax-rules db-use ((root options c) (db-use-p root options c))
    ((root c) (db-use-p root (list) c)))

  (define (db-txn-call-read env c)
    "db-env procedure:{db-txn -> any:result} -> any:result
     call c with a new read transaction.
     the transaction is automatically finished on return with db-txn-abort"
    (let (txn (db-txn-begin env)) (exception-always (db-txn-abort txn) (c txn))))

  (define (db-txn-call-write env c)
    "db-env procedure:{db-txn -> any:result} -> any:result
     call c with a new write transaction.
     the transaction is automatically committed on return if not already aborted or committed.
     the transaction is aborted when an unhandled exception occurs"
    (let (txn (db-txn-write-begin env))
      (exception-intercept-if (c txn) (db-txn-abort txn)
        (if (db-txn-active? txn) (db-txn-commit txn)))))

  (define (db-record->values type a) "db-type db-record -> list:((field-offset . value) ...)"
    (vector-map-with-index (l (index a) (and a (pair index a))) (db-record->vector type a))))
