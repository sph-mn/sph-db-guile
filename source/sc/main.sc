(sc-comment
  "sph-db-guile basically registers scheme procedures that when called execute specific c-functions that manage calls to sph-db")

(pre-include
  "libguile.h" "sph-db.h" "sph-db-extra.h" "./foreign/sph/one.c" "./foreign/sph/guile.c" "./helper.c")

(define (scm-db-txn-active? a) (SCM SCM) (return (scm-from-bool (scm->db-txn a))))
(define (scm-db-env-open? a) (SCM SCM) (return (scm-from-bool (: (scm->db-env a) is-open))))
(define (scm-db-env-root a) (SCM SCM) (return (scm-from-utf8-string (: (scm->db-env a) root))))

(define (scm-db-env-maxkeysize a) (SCM SCM)
  (return (scm-from-uint32 (: (scm->db-env a) maxkeysize))))

(define (scm-db-env-format a) (SCM SCM) (return (scm-from-uint32 (: (scm->db-env a) format))))
(define (scm-db-type-id a) (SCM SCM) (return (scm-from-uint (: (scm->db-type a) id))))
(define (scm-db-type-name a) (SCM SCM) (return (scm-from-utf8-string (: (scm->db-type a) name))))
(define (scm-db-type-flags a) (SCM SCM) (return (scm-from-uint8 (: (scm->db-type a) flags))))

(define (scm-db-type-virtual? a) (SCM SCM)
  (return (scm-from-bool (db-type-is-virtual (scm->db-type a)))))

(define (scm-db-type-fields a) (SCM SCM)
  (declare
    i db-fields-len-t
    type db-type-t*
    fields-len db-fields-len-t
    field db-field-t
    fields db-field-t*
    result SCM)
  (set
    result SCM-EOL
    type (scm->db-type a)
    fields type:fields
    fields-len type:fields-len
    i fields-len)
  (while i
    (set
      i (- i 1)
      field (array-get fields i)
      result
      (scm-cons
        (scm-cons (scm-from-utf8-stringn field.name field.name-len) (db-field-type->scm field.type))
        result)))
  (return result))

(define (scm-db-type-indices a) (SCM SCM)
  (declare
    field db-field-t
    i-index db-indices-len-t
    i-field db-fields-len-t
    index db-index-t*
    index-fields db-fields-len-t*
    index-fields-len db-fields-len-t
    indices db-index-t*
    indices-len db-indices-len-t
    result SCM
    scm-fields SCM
    type db-type-t*)
  (set
    result SCM-EOL
    type (scm->db-type a)
    indices-len type:indices-len
    indices type:indices)
  (for ((set i-field 0) (< i-field indices-len) (set i-index (+ 1 i-index)))
    (set
      index (+ i-index indices)
      scm-fields SCM-EOL)
    (for ((set i-field 0) (< i-field index:fields-len) (set i-field (+ 1 i-field)))
      (set
        field (array-get type:fields (array-get index:fields i-field))
        scm-fields
        (scm-cons
          (scm-list-2
            (scm-from-utf8-stringn field.name field.name-len) (db-field-type->scm field.type))
          scm-fields)))
    (set result (scm-cons (scm-cons scm-fields result) result)))
  (return result))

(define (scm-db-open scm-root scm-options) (SCM SCM SCM)
  status-declare
  (db-env-declare env)
  (declare
    options db-open-options-t
    options-pointer db-open-options-t*
    a SCM
    root uint8_t*)
  (set
    root 0
    root (scm->utf8-string scm-root))
  (status-require (db-env-new &env))
  (if (or (scm-is-undefined scm-options) (scm-is-null scm-options)) (set options-pointer 0)
    (begin
      (db-open-options-set-defaults &options)
      (scm-options-get scm-options "is-read-only" a)
      (if (scm-is-bool a) (set options.is-read-only (scm-is-true a)))
      (scm-options-get scm-options "maximum-reader-count" a)
      (if (scm-is-integer a) (set options.maximum-reader-count (scm->uint a)))
      (scm-options-get scm-options "filesystem-has-ordered-writes" a)
      (if (scm-is-bool a) (set options.filesystem-has-ordered-writes (scm-is-true a)))
      (scm-options-get scm-options "env-open-flags" a)
      (if (scm-is-integer a) (set options.env-open-flags (scm->uint a)))
      (scm-options-get scm-options "file-permissions" a)
      (if (scm-is-integer a) (set options.file-permissions (scm->uint a)))
      (set options-pointer &options)))
  (set status (db-open root options-pointer env))
  (label exit
    (free root)
    (if status-is-success (return (db-env->scm env))
      (begin
        (db-close env)
        (free env)
        (status->scm-error status)))))

(define (scm-db-close scm-env) (SCM SCM)
  (db-env-declare env)
  (set env (scm->db-env scm-env))
  (scm-gc)
  (db-close env)
  (free env)
  (return SCM-UNSPECIFIED))

(define (scm-db-statistics scm-txn) (SCM SCM)
  status-declare
  (declare
    b SCM
    a db-statistics-t)
  (status-require (db-statistics (pointer-get (scm->db-txn scm-txn)) &a))
  (set
    b SCM-EOL
    b (scm-acons (scm-from-latin1-symbol "system") (scm-from-mdb-stat a.system) b)
    b (scm-acons (scm-from-latin1-symbol "records") (scm-from-mdb-stat a.records) b)
    b (scm-acons (scm-from-latin1-symbol "relation-lr") (scm-from-mdb-stat a.relation-lr) b)
    b (scm-acons (scm-from-latin1-symbol "relation-rl") (scm-from-mdb-stat a.relation-rl) b)
    b (scm-acons (scm-from-latin1-symbol "relation-ll") (scm-from-mdb-stat a.relation-ll) b))
  (label exit
    (status->scm-return b)))

(define (scm-db-txn-abort scm-txn) (SCM SCM)
  (db-guile-selections-free)
  (declare txn db-txn-t*)
  (set txn (scm->db-txn scm-txn))
  (db-txn-abort txn)
  (free txn)
  (scm-foreign-object-set-x scm-txn 0 0)
  (return SCM-UNSPECIFIED))

(define (scm-db-txn-commit scm-txn) (SCM SCM)
  "note that commit frees cursors. db-guile-selections-free closes cursors.
  if db-guile-selections-free is called after db-txn-commit a double free occurs"
  status-declare
  (db-guile-selections-free)
  (declare txn db-txn-t*)
  (set txn (scm->db-txn scm-txn))
  (status-require (db-txn-commit txn))
  (free txn)
  (scm-foreign-object-set-x scm-txn 0 0)
  (label exit
    (status->scm-return SCM-UNSPECIFIED)))

(define (scm-db-txn-begin scm-env) (SCM SCM)
  status-declare
  (declare txn db-txn-t*)
  (set txn 0)
  (db-calloc txn 1 (sizeof db-txn-t))
  (set txn:env (scm->db-env scm-env))
  (status-require (db-txn-begin txn))
  (label exit
    (if status-is-success (return (db-txn->scm txn))
      (begin
        (free txn)
        (status->scm-error status)
        (return SCM-UNSPECIFIED)))))

(define (scm-db-txn-write-begin scm-env) (SCM SCM)
  status-declare
  (declare txn db-txn-t*)
  (set txn 0)
  (db-calloc txn 1 (sizeof db-txn-t))
  (set txn:env (scm->db-env scm-env))
  (status-require (db-txn-write-begin txn))
  (label exit
    (if status-is-success (return (db-txn->scm txn))
      (begin
        (free txn)
        (status->scm-error status)
        (return SCM-UNSPECIFIED)))))

(define (scm-db-status-description id-status id-group) (SCM SCM SCM)
  status-declare
  (status-set-both (scm->int id-group) (scm->int id-status))
  (scm-from-latin1-string (db-status-description status)))

(define (scm-db-status-group-id->name a) (SCM SCM)
  (scm-from-latin1-symbol (db-status-group-id->name (scm->int a))))

(define (scm-db-type-create scm-env scm-name scm-fields scm-flags) (SCM SCM SCM SCM SCM)
  status-declare
  (declare
    field-name uint8-t*
    field-name-len db-name-len-t
    field-type db-field-type-t
    fields db-field-t*
    fields-len db-fields-len-t
    flags uint8-t
    i db-fields-len-t
    name uint8-t*
    scm-field SCM
    type db-type-t*)
  (set
    name (scm->utf8-string scm-name)
    flags
    (if* (scm-is-undefined scm-flags) 0
      (scm->uint8 scm-flags))
    fields-len (scm->uint (scm-length scm-fields)))
  (db-calloc fields fields-len (sizeof db-field-t))
  (for
    ( (set i 0) (< i fields-len)
      (set
        i (+ 1 i)
        scm-fields (scm-tail scm-fields)))
    (set
      scm-field (scm-first scm-fields)
      field-name (scm->utf8-string (scm-first scm-field))
      field-name-len (strlen field-name)
      field-type (scm->db-field-type (scm-tail scm-field)))
    (db-field-set (array-get fields i) field-type field-name field-name-len))
  (status-require (db-type-create (scm->db-env scm-env) name fields fields-len flags &type))
  (label exit
    (status->scm-return (db-type->scm type (scm->db-env scm-env)))))

(define (scm-db-type-get scm-env scm-name-or-id) (SCM SCM SCM)
  (declare
    type db-type-t*
    name uint8-t*)
  (if (scm-is-string scm-name-or-id)
    (set
      name (scm->utf8-string scm-name-or-id)
      type (db-type-get (scm->db-env scm-env) name))
    (set type (db-type-get-by-id (scm->db-env scm-env) (scm->uint scm-name-or-id))))
  (return
    (if* type (db-type->scm type (scm->db-env scm-env))
      SCM-BOOL-F)))

(define (scm-db-type-delete scm-type) (SCM SCM)
  status-declare
  (status-require
    (db-type-delete
      (convert-type (scm-foreign-object-ref scm-type 1) db-env-t*) (: (scm->db-type scm-type) id)))
  (label exit
    (status->scm-return SCM-UNSPECIFIED)))

(define (db-guile-init) void
  "prepare scm valuaes and register guile bindings"
  (declare
    type-slots SCM
    scm-symbol-data SCM)
  (set
    scm-symbol-data (scm-from-latin1-symbol "data")
    scm-symbol-binary (scm-from-latin1-symbol "binary")
    scm-symbol-string (scm-from-latin1-symbol "string")
    scm-symbol-float32 (scm-from-latin1-symbol "float32")
    scm-symbol-float64 (scm-from-latin1-symbol "float64")
    scm-symbol-int8 (scm-from-latin1-symbol "int8")
    scm-symbol-int16 (scm-from-latin1-symbol "int16")
    scm-symbol-int32 (scm-from-latin1-symbol "int32")
    scm-symbol-int64 (scm-from-latin1-symbol "int64")
    scm-symbol-uint8 (scm-from-latin1-symbol "uint8")
    scm-symbol-uint16 (scm-from-latin1-symbol "uint16")
    scm-symbol-uint32 (scm-from-latin1-symbol "uint32")
    scm-symbol-uint64 (scm-from-latin1-symbol "uint64")
    scm-symbol-string8 (scm-from-latin1-symbol "string8")
    scm-symbol-string16 (scm-from-latin1-symbol "string16")
    scm-symbol-string32 (scm-from-latin1-symbol "string32")
    scm-symbol-string64 (scm-from-latin1-symbol "string64")
    scm-rnrs-raise (scm-c-public-ref "rnrs exceptions" "raise")
    type-slots (scm-list-1 scm-symbol-data)
    scm-type-env (scm-make-foreign-object-type (scm-from-latin1-symbol "db-env") type-slots 0)
    scm-type-txn (scm-make-foreign-object-type (scm-from-latin1-symbol "db-txn") type-slots 0)
    scm-type-index (scm-make-foreign-object-type (scm-from-latin1-symbol "db-index") type-slots 0)
    type-slots (scm-list-2 scm-symbol-data (scm-from-latin1-symbol "env"))
    scm-type-type (scm-make-foreign-object-type (scm-from-latin1-symbol "db-type") type-slots 0)
    scm-type-selection
    (scm-make-foreign-object-type (scm-from-latin1-symbol "db-selection") type-slots 0))
  ; exports
  scm-c-define-procedure-c-init
  (define m SCM (scm-c-resolve-module "sph db"))
  (scm-c-module-define m "db-type-flag-virtual" (scm-from-uint db-type-flag-virtual))
  (scm-c-define-procedure-c
    "db-open" 1 1 0 scm-db-open "string:root [((key . value) ...):options] ->")
  (scm-c-define-procedure-c "db-close" 1 0 0 scm-db-close "deinitialises the database handle")
  (scm-c-define-procedure-c "db-env-open?" 1 0 0 scm-db-env-open? "")
  (scm-c-define-procedure-c "db-env-maxkeysize" 1 0 0 scm-db-env-maxkeysize "")
  (scm-c-define-procedure-c "db-env-root" 1 0 0 scm-db-env-root "")
  (scm-c-define-procedure-c "db-env-format" 1 0 0 scm-db-env-format "")
  (scm-c-define-procedure-c "db-statistics" 1 0 0 scm-db-statistics "")
  (scm-c-define-procedure-c "db-txn-begin" 1 0 0 scm-db-txn-begin "-> db-txn")
  (scm-c-define-procedure-c "db-txn-write-begin" 1 0 0 scm-db-txn-write-begin "-> db-txn")
  (scm-c-define-procedure-c "db-txn-abort" 1 0 0 scm-db-txn-abort "db-txn -> unspecified")
  (scm-c-define-procedure-c "db-txn-commit" 1 0 0 scm-db-txn-commit "db-txn -> unspecified")
  (scm-c-define-procedure-c "db-txn-active?" 1 0 0 scm-db-txn-active? "db-txn -> boolean")
  (scm-c-define-procedure-c
    "db-status-description"
    2 0 0 scm-db-status-description "integer:id-status integer:id-group -> string")
  (scm-c-define-procedure-c
    "db-status-group-id->name" 1 0 0 scm-db-status-group-id->name "integer -> symbol")
  (scm-c-define-procedure-c "db-type-create" 3 1 0 scm-db-type-create "")
  (scm-c-define-procedure-c "db-type-delete" 1 0 0 scm-db-type-delete "")
  (scm-c-define-procedure-c "db-type-get" 2 0 0 scm-db-type-get "")
  (scm-c-define-procedure-c "db-type-id" 1 0 0 scm-db-type-id "")
  (scm-c-define-procedure-c "db-type-name" 1 0 0 scm-db-type-name "")
  (scm-c-define-procedure-c "db-type-indices" 1 0 0 scm-db-type-indices "")
  (scm-c-define-procedure-c "db-type-fields" 1 0 0 scm-db-type-fields "")
  (scm-c-define-procedure-c "db-type-virtual?" 1 0 0 scm-db-type-virtual? "")
  (scm-c-define-procedure-c "db-type-flags" 1 0 0 scm-db-type-flags ""))