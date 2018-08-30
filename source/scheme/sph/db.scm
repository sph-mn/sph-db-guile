(library (sph db)
  (export
    db-close
    db-open
    db-relation-field-names
    db-relation-label
    db-relation-layout
    db-relation-left
    db-relation-ordinal
    db-relation-right
    db-use
    db-use-p)
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
      ; used in the shared library
      write
      read))

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
    (let (env (db-open root option)) (exception-always (db-close env) (c env))))

  (define-syntax-rules db-use
    ( (root ((option-name option-value) ...) c)
      (db-use-p root (list (pair (quote option-name) option-value) ...) c))
    ((root c) (db-use-p root (list) c))))
