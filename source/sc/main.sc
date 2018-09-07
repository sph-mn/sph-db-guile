(sc-comment
  "sph-db-guile registers scheme procedures that when called execute specific c-functions that manage calls to sph-db")

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

(define (scm-db-type-indices scm-type) (SCM SCM)
  (declare
    i db-indices-len-t
    indices db-index-t*
    indices-len db-indices-len-t
    result SCM
    type db-type-t*)
  (set
    result SCM-EOL
    type (scm->db-type scm-type)
    indices-len type:indices-len
    indices type:indices)
  (for ((set i 0) (< i indices-len) (set i (+ 1 i)))
    (set result (scm-cons (db-index->scm-fields (+ i indices)) result)))
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
    root (scm->utf8-stringn scm-root 0))
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
  (status-require (db-helper-calloc (sizeof db-txn-t) &txn))
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
  (status-require (db-helper-calloc (sizeof db-txn-t) &txn))
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
  (set name (scm->utf8-stringn scm-name 0))
  (scm-dynwind-begin 0)
  (scm-dynwind-free name)
  (set
    flags
    (if* (scm-is-undefined scm-flags) 0
      (scm->uint8 scm-flags))
    fields-len (scm->uint (scm-length scm-fields)))
  (status-require (db-helper-calloc (* fields-len (sizeof db-field-t)) &fields))
  (scm-dynwind-free fields)
  (for
    ( (set i 0) (< i fields-len)
      (set
        i (+ 1 i)
        scm-fields (scm-tail scm-fields)))
    (set
      scm-field (scm-first scm-fields)
      field-name (scm->utf8-stringn (scm-first scm-field) 0))
    (scm-dynwind-free field-name)
    (set
      field-name-len (strlen field-name)
      field-type (scm->db-field-type (scm-tail scm-field)))
    (db-field-set (array-get fields i) field-type field-name field-name-len))
  (status-require (db-type-create (scm->db-env scm-env) name fields fields-len flags &type))
  (label exit
    (scm-dynwind-end)
    (status->scm-return (db-type->scm type (scm->db-env scm-env)))))

(define (scm-db-type-get scm-env scm-name-or-id) (SCM SCM SCM)
  (declare
    type db-type-t*
    name uint8-t*)
  (scm-dynwind-begin 0)
  (if (scm-is-string scm-name-or-id)
    (begin
      (set name (scm->utf8-stringn scm-name-or-id 0))
      (scm-dynwind-free name)
      (set type (db-type-get (scm->db-env scm-env) name)))
    (set type (db-type-get-by-id (scm->db-env scm-env) (scm->uint scm-name-or-id))))
  (scm-dynwind-end)
  (return
    (if* type (db-type->scm type (scm->db-env scm-env))
      SCM-BOOL-F)))

(define (scm-db-type-delete scm-type) (SCM SCM)
  status-declare
  (status-require (db-type-delete (scm-type->db-env scm-type) (: (scm->db-type scm-type) id)))
  (label exit
    (status->scm-return SCM-UNSPECIFIED)))

(define (scm-db-index-create scm-type scm-fields) (SCM SCM SCM)
  status-declare
  (declare
    fields db-fields-len-t*
    fields-len db-fields-len-t
    index db-index-t*)
  (status-require (scm->field-offsets scm-type scm-fields &fields &fields-len))
  (status-require
    (db-index-create (scm-type->db-env scm-type) (scm->db-type scm-type) fields fields-len &index))
  (label exit
    (status->scm-return (db-index->scm index (scm-type->db-env scm-type)))))

(define (scm-db-index-get scm-type scm-fields) (SCM SCM SCM)
  status-declare
  (declare
    fields db-fields-len-t*
    fields-len db-fields-len-t
    index db-index-t*)
  (status-require (scm->field-offsets scm-type scm-fields &fields &fields-len))
  (set index (db-index-get (scm->db-type scm-type) fields fields-len))
  (label exit
    (status->scm-return
      (if* index (db-index->scm index (scm-type->db-env scm-type))
        SCM-BOOL-F))))

(define (scm-db-index-delete scm-index) (SCM SCM)
  status-declare
  (status-require (db-index-delete (scm-index->db-env scm-index) (scm->db-index scm-index)))
  (label exit
    (status->scm-return SCM-UNSPECIFIED)))

(define (scm-db-index-rebuild scm-index) (SCM SCM)
  status-declare
  (status-require (db-index-rebuild (scm-index->db-env scm-index) (scm->db-index scm-index)))
  (label exit
    (status->scm-return SCM-UNSPECIFIED)))

(define (scm-db-index-fields scm-index) (SCM SCM)
  (return (db-index->scm-fields (scm->db-index scm-index))))

(define (scm-db-record-create scm-txn scm-type scm-values) (SCM SCM SCM SCM)
  status-declare
  (db-record-values-declare values)
  (declare
    field-data void*
    field-data-size size-t
    field-data-is-ref boolean
    scm-value SCM
    field-offset db-fields-len-t
    i db-fields-len-t
    result-id db-id-t
    type db-type-t*)
  (set type (scm->db-type scm-type))
  (scm-dynwind-begin 0)
  (status-require (db-record-values-new type &values))
  (scm-dynwind-unwind-handler
    (convert-type db-record-values-free (function-pointer void void*)) &values SCM-F-WIND-EXPLICITLY)
  (sc-comment "set field values")
  (while (not (scm-is-null scm-values))
    (set scm-value (scm-first scm-values))
    (status-require (scm->field-offset (scm-first scm-value) type &field-offset))
    (status-require
      (scm->field-data
        (scm-tail scm-value)
        (: (+ field-offset type:fields) type) &field-data &field-data-size &field-data-is-ref))
    (if (not field-data-is-ref) (scm-dynwind-unwind-handler free field-data SCM-F-WIND-EXPLICITLY))
    (db-record-values-set &values field-offset field-data field-data-size)
    (set scm-values (scm-tail scm-values)))
  (sc-comment "save")
  (status-require (db-record-create (pointer-get (scm->db-txn scm-txn)) values &result-id))
  (label exit
    (scm-dynwind-end)
    (status->scm-return (scm-from-uint result-id))))

(define (db-guile-ordinal-generator state) (db-ordinal-t void*)
  (declare
    scm-state SCM
    scm-generator SCM
    scm-result SCM)
  (set
    scm-state (pointer-get (convert-type state SCM*))
    scm-generator (scm-first scm-state)
    scm-result (scm-apply-0 scm-generator (scm-tail scm-state))
    (pointer-get (convert-type state SCM*)) (scm-cons scm-generator scm-result))
  (return (scm->uint (scm-first scm-result))))

(define
  (scm-db-relation-ensure
    scm-txn scm-left scm-right scm-label scm-ordinal-generator scm-ordinal-state)
  (SCM SCM SCM SCM SCM SCM SCM)
  status-declare
  (declare
    left db-ids-t
    right db-ids-t
    label db-ids-t
    scm-state SCM
    ordinal-generator db-relation-ordinal-generator-t
    ordinal-state void*
    ordinal-value db-ordinal-t)
  (status-require (scm->db-ids scm-left &left))
  (status-require (scm->db-ids scm-right &right))
  (status-require (scm->db-ids scm-label &label))
  (if (scm-is-true (scm-procedure? scm-ordinal-generator))
    (set
      scm-state
      (scm-cons
        scm-ordinal-generator
        (if* (scm-is-true (scm-list? scm-ordinal-state)) scm-ordinal-state
          (scm-list-1 scm-ordinal-state)))
      ordinal-state &scm-state
      ordinal-generator db-guile-ordinal-generator)
    (set
      ordinal-generator 0
      ordinal-value
      (if* (scm-is-undefined scm-ordinal-state) 0
        (scm->uint scm-ordinal-state))
      ordinal-state &ordinal-value))
  (db-relation-ensure
    (pointer-get (scm->db-txn scm-txn)) left right label ordinal-generator ordinal-state)
  (label exit
    (status->scm-return SCM-BOOL-T)))

(define (scm-db-id-type a) (SCM SCM) (scm-from-uint (db-id-type (scm->uint a))))
(define (scm-db-id-element a) (SCM SCM) (scm-from-uint (db-id-element (scm->uint a))))

(define (scm-db-id-add-type a type) (SCM SCM SCM)
  (scm-from-uint (db-id-add-type (scm->uint a) (scm->uint type))))

(define (scm-db-record-get scm-txn scm-ids) (SCM SCM SCM)
  status-declare
  (declare
    ids db-ids-t
    records db-records-t
    result SCM)
  (scm-dynwind-begin 0)
  (status-require (scm->db-ids scm-ids &ids))
  (scm-dynwind-free ids.start)
  (status-require (db-records-new (db-ids-length ids) &records))
  (scm-dynwind-free records.start)
  (status-require (db-record-get (pointer-get (scm->db-txn scm-txn)) ids &records))
  (set result (db-records->scm records))
  (label exit
    (scm-dynwind-end)
    (status->scm-return result)))

(define
  (scm-db-relation-select scm-txn scm-left scm-right scm-label scm-retrieve scm-ordinal scm-offset)
  (SCM SCM SCM SCM SCM SCM SCM SCM)
  status-declare
  (declare
    left db-ids-t
    right db-ids-t
    scm-ordinal-min SCM
    relations->scm (function-pointer SCM db-relations-t)
    scm-ordinal-max SCM
    label db-ids-t
    left-pointer db-ids-t*
    right-pointer db-ids-t*
    label-pointer db-ids-t*
    ordinal db-ordinal-condition-t
    ordinal-pointer db-ordinal-condition-t*
    offset uint32-t
    selection db-guile-relation-selection-t*)
  (if (or (scm-is-null scm-left) (scm-is-null scm-right) (scm-is-null scm-label))
    (return (db-selection->scm 0)))
  (sc-comment "dont start the dynwind context sooner or it might not be finished")
  (scm-dynwind-begin 0)
  (sc-comment "left/right/label")
  (if (scm-is-pair scm-left)
    (begin
      (status-require (scm->db-ids scm-left &left))
      (set left-pointer &left))
    (set left-pointer 0))
  (if (scm-is-pair scm-right)
    (begin
      (status-require (scm->db-ids scm-right &right))
      (set right-pointer &right))
    (set right-pointer 0))
  (if (scm-is-pair scm-right)
    (begin
      (status-require (scm->db-ids scm-label &label))
      (set label-pointer &label))
    (set label-pointer 0))
  (sc-comment "ordinal")
  (if (scm-is-true (scm-list? scm-ordinal))
    (set
      scm-ordinal-min (scm-assoc-ref scm-ordinal scm-symbol-min)
      scm-ordinal-max (scm-assoc-ref scm-ordinal scm-symbol-max)
      ordinal.min
      (if* (scm-is-integer scm-ordinal-min) (scm->uint scm-ordinal-min)
        0)
      ordinal.max
      (if* (scm-is-integer scm-ordinal-max) (scm->uint scm-ordinal-max)
        0)
      ordinal-pointer &ordinal)
    (set ordinal-pointer 0))
  (sc-comment "offset")
  (set offset
    (if* (scm-is-integer scm-offset) (scm->uint32 scm-offset)
      0))
  (sc-comment "retrieve")
  (set relations->scm
    (if* (scm-is-symbol scm-retrieve)
      (case* scm-is-eq scm-retrieve
        (scm-symbol-right db-relations->scm-retrieve-right)
        (scm-symbol-left db-relations->scm-retrieve-left)
        (scm-symbol-label db-relations->scm-retrieve-label)
        (scm-symbol-ordinal db-relations->scm-retrieve-ordinal)
        (else 0))
      db-relations->scm))
  (if (not relations->scm)
    (status-set-both-goto db-status-group-db-guile status-id-invalid-argument))
  (sc-comment "db-relation-select")
  (status-require (db-helper-malloc (sizeof db-guile-relation-selection-t) &selection))
  (scm-dynwind-unwind-handler free selection 0)
  (status-require
    (db-relation-select
      (pointer-get (scm->db-txn scm-txn))
      left-pointer right-pointer label-pointer ordinal-pointer offset &selection:selection))
  (set
    selection:left left
    selection:right right
    selection:label label
    selection:relations->scm relations->scm)
  (label exit
    (scm-dynwind-end)
    (status->scm-return (db-selection->scm selection))))

; db-record-ref
; db-record->record
(define (scm-db-relation-read scm-selection scm-count) (SCM SCM SCM))
(define (scm-db-record-select scm-txn) (SCM SCM))
(define (scm-db-record-read scm-txn) (SCM SCM))
(define (scm-db-record-ref scm-type scm-record scm-field) (SCM SCM SCM SCM))
(define (scm-db-index-select scm-txn) (SCM SCM))
(define (scm-db-index-read scm-txn) (SCM SCM))
(define (scm-db-record-index-select scm-txn) (SCM SCM))
(define (scm-db-record-index-read scm-txn) (SCM SCM))
(define (scm-db-record-virtual scm-data) (SCM SCM))

(define (db-guile-init) void
  "prepare scm valuaes and register guile bindings"
  (declare
    type-slots SCM
    scm-symbol-data SCM)
  (set
    scm-rnrs-raise (scm-c-public-ref "rnrs exceptions" "raise")
    scm-symbol-min (scm-from-latin1-symbol "min")
    scm-symbol-max (scm-from-latin1-symbol "max")
    scm-symbol-binary (scm-from-latin1-symbol "binary")
    scm-symbol-data (scm-from-latin1-symbol "data")
    scm-symbol-float32 (scm-from-latin1-symbol "float32")
    scm-symbol-float64 (scm-from-latin1-symbol "float64")
    scm-symbol-int16 (scm-from-latin1-symbol "int16")
    scm-symbol-int32 (scm-from-latin1-symbol "int32")
    scm-symbol-int64 (scm-from-latin1-symbol "int64")
    scm-symbol-int8 (scm-from-latin1-symbol "int8")
    scm-symbol-label (scm-from-latin1-symbol "label")
    scm-symbol-left (scm-from-latin1-symbol "left")
    scm-symbol-ordinal (scm-from-latin1-symbol "ordinal")
    scm-symbol-right (scm-from-latin1-symbol "right")
    scm-symbol-string (scm-from-latin1-symbol "string")
    scm-symbol-string16 (scm-from-latin1-symbol "string16")
    scm-symbol-string32 (scm-from-latin1-symbol "string32")
    scm-symbol-string64 (scm-from-latin1-symbol "string64")
    scm-symbol-string8 (scm-from-latin1-symbol "string8")
    scm-symbol-uint16 (scm-from-latin1-symbol "uint16")
    scm-symbol-uint32 (scm-from-latin1-symbol "uint32")
    scm-symbol-uint64 (scm-from-latin1-symbol "uint64")
    scm-symbol-uint8 (scm-from-latin1-symbol "uint8")
    type-slots (scm-list-1 scm-symbol-data)
    scm-type-env (scm-make-foreign-object-type (scm-from-latin1-symbol "db-env") type-slots 0)
    scm-type-txn (scm-make-foreign-object-type (scm-from-latin1-symbol "db-txn") type-slots 0)
    type-slots (scm-list-2 scm-symbol-data (scm-from-latin1-symbol "env"))
    scm-type-type (scm-make-foreign-object-type (scm-from-latin1-symbol "db-type") type-slots 0)
    scm-type-index (scm-make-foreign-object-type (scm-from-latin1-symbol "db-index") type-slots 0)
    type-slots
    (scm-list-3 (scm-from-latin1-symbol "id") (scm-from-latin1-symbol "size") scm-symbol-data)
    scm-type-record (scm-make-foreign-object-type (scm-from-latin1-symbol "db-record") type-slots 0)
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
  (scm-c-define-procedure-c "db-type-flags" 1 0 0 scm-db-type-flags "")
  (scm-c-define-procedure-c "db-index-create" 2 0 0 scm-db-index-create "")
  (scm-c-define-procedure-c "db-index-delete" 1 0 0 scm-db-index-delete "")
  (scm-c-define-procedure-c "db-index-get" 2 0 0 scm-db-index-get "")
  (scm-c-define-procedure-c "db-index-rebuild" 1 0 0 scm-db-index-rebuild "")
  (scm-c-define-procedure-c "db-index-fields" 1 0 0 scm-db-index-fields "")
  (scm-c-define-procedure-c "db-id-type" 1 0 0 scm-db-id-type "")
  (scm-c-define-procedure-c "db-id-element" 1 0 0 scm-db-id-element "")
  (scm-c-define-procedure-c "db-id-add-type" 2 0 0 scm-db-id-add-type "")
  (scm-c-define-procedure-c
    "db-record-create"
    3 0 0 scm-db-record-create "db-txn db-type list:((field-id . value) ...) -> integer")
  (scm-c-define-procedure-c
    "db-relation-ensure"
    4
    2
    0
    scm-db-relation-ensure "db-txn list:left list:right list:label [ordinal-generator ordinal-state]"))