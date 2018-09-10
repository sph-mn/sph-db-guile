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
        (scm-cons
          (scm-from-utf8-stringn field.name field.name-len) (scm-from-db-field-type field.type))
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
    (set result (scm-cons (scm-from-db-index-fields (+ i indices)) result)))
  (return result))

(define (scm-db-status-description id-status id-group) (SCM SCM SCM)
  status-declare
  (status-set-both (scm->int id-group) (scm->int id-status))
  (scm-from-latin1-string (db-status-description status)))

(define (scm-db-status-group-id->name a) (SCM SCM)
  (scm-from-latin1-symbol (db-status-group-id->name (scm->int a))))

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
    (if status-is-success (return (scm-from-db-env env))
      (begin
        (db-close env)
        (free env)
        (scm-from-status-error status)))))

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
    (scm-from-status-return b)))

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
    (scm-from-status-return SCM-UNSPECIFIED)))

(define (scm-db-txn-begin scm-env) (SCM SCM)
  status-declare
  (declare txn db-txn-t*)
  (set txn 0)
  (status-require (db-helper-calloc (sizeof db-txn-t) &txn))
  (set txn:env (scm->db-env scm-env))
  (status-require (db-txn-begin txn))
  (label exit
    (if status-is-success (return (scm-from-db-txn txn))
      (begin
        (free txn)
        (scm-from-status-error status)
        (return SCM-UNSPECIFIED)))))

(define (scm-db-txn-write-begin scm-env) (SCM SCM)
  status-declare
  (declare txn db-txn-t*)
  (set txn 0)
  (status-require (db-helper-calloc (sizeof db-txn-t) &txn))
  (set txn:env (scm->db-env scm-env))
  (status-require (db-txn-write-begin txn))
  (label exit
    (if status-is-success (return (scm-from-db-txn txn))
      (begin
        (free txn)
        (scm-from-status-error status)
        (return SCM-UNSPECIFIED)))))

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
    (scm-from-status-return (scm-from-db-type type))))

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
    (if* type (scm-from-db-type type)
      SCM-BOOL-F)))

(define (scm-db-type-delete scm-env scm-type) (SCM SCM SCM)
  status-declare
  (status-require (db-type-delete (scm->db-env scm-env) (: (scm->db-type scm-type) id)))
  (label exit
    (scm-from-status-return SCM-UNSPECIFIED)))

(define (scm-db-index-create scm-env scm-type scm-fields) (SCM SCM SCM SCM)
  status-declare
  (declare
    fields db-fields-len-t*
    fields-len db-fields-len-t
    index db-index-t*)
  (status-require (scm->field-offsets scm-type scm-fields &fields &fields-len))
  (status-require
    (db-index-create (scm->db-env scm-env) (scm->db-type scm-type) fields fields-len &index))
  (label exit
    (scm-from-status-return (scm-from-db-index index))))

(define (scm-db-index-get scm-env scm-type scm-fields) (SCM SCM SCM SCM)
  status-declare
  (declare
    fields db-fields-len-t*
    fields-len db-fields-len-t
    index db-index-t*)
  (status-require (scm->field-offsets scm-type scm-fields &fields &fields-len))
  (set index (db-index-get (scm->db-type scm-type) fields fields-len))
  (label exit
    (scm-from-status-return
      (if* index (scm-from-db-index index)
        SCM-BOOL-F))))

(define (scm-db-index-delete scm-env scm-index) (SCM SCM SCM)
  status-declare
  (status-require (db-index-delete (scm->db-env scm-env) (scm->db-index scm-index)))
  (label exit
    (scm-from-status-return SCM-UNSPECIFIED)))

(define (scm-db-index-rebuild scm-env scm-index) (SCM SCM SCM)
  status-declare
  (status-require (db-index-rebuild (scm->db-env scm-env) (scm->db-index scm-index)))
  (label exit
    (scm-from-status-return SCM-UNSPECIFIED)))

(define (scm-db-index-fields scm-index) (SCM SCM)
  (return (scm-from-db-index-fields (scm->db-index scm-index))))

(define (scm-db-record-create scm-txn scm-type scm-values) (SCM SCM SCM SCM)
  status-declare
  (db-record-values-declare values)
  (declare
    field-data void*
    field-data-size size-t
    field-data-needs-free boolean
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
        (: (+ field-offset type:fields) type) &field-data &field-data-size &field-data-needs-free))
    (if field-data-needs-free (scm-dynwind-free field-data))
    (db-record-values-set &values field-offset field-data field-data-size)
    (set scm-values (scm-tail scm-values)))
  (sc-comment "save")
  (status-require (db-record-create (pointer-get (scm->db-txn scm-txn)) values &result-id))
  (label exit
    (scm-dynwind-end)
    (scm-from-status-return (scm-from-uint result-id))))

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
    (scm-from-status-return SCM-BOOL-T)))

(define (scm-db-id-type a) (SCM SCM) (scm-from-uint (db-id-type (scm->uint a))))
(define (scm-db-id-element a) (SCM SCM) (scm-from-uint (db-id-element (scm->uint a))))

(define (scm-db-id-add-type a type) (SCM SCM SCM)
  (scm-from-uint (db-id-add-type (scm->uint a) (scm->uint type))))

(define (scm-db-record-get scm-txn scm-ids) (SCM SCM SCM)
  status-declare
  (declare
    ids db-ids-t
    records db-records-t
    result SCM
    txn db-txn-t)
  (set txn (pointer-get (scm->db-txn scm-txn)))
  (scm-dynwind-begin 0)
  (status-require (scm->db-ids scm-ids &ids))
  (scm-dynwind-free ids.start)
  (status-require (db-records-new (db-ids-length ids) &records))
  (scm-dynwind-free records.start)
  (status-require (db-record-get txn ids &records))
  (set result (scm-from-db-records records))
  (label exit
    (scm-dynwind-end)
    (scm-from-status-return result)))

(define (scm-db-relation-select scm-txn scm-left scm-right scm-label scm-retrieve scm-ordinal)
  (SCM SCM SCM SCM SCM SCM SCM)
  status-declare
  (declare
    label db-ids-t
    label-pointer db-ids-t*
    left db-ids-t
    left-pointer db-ids-t*
    offset uint32-t
    ordinal db-ordinal-condition-t
    ordinal-pointer db-ordinal-condition-t*
    right db-ids-t
    right-pointer db-ids-t*
    scm-from-relations (function-pointer SCM db-relations-t)
    scm-ordinal-max SCM
    scm-ordinal-min SCM
    scm-selection SCM
    selection db-guile-relation-selection-t*)
  (if (or (scm-is-null scm-left) (scm-is-null scm-right) (scm-is-null scm-label))
    (return (scm-from-db-selection 0)))
  (sc-comment "dont call dynwind-begin sooner or dynwind-end might not be called")
  (scm-dynwind-begin 0)
  (sc-comment "left/right/label")
  (if (scm-is-pair scm-left)
    (begin
      (status-require (scm->db-ids scm-left &left))
      (scm-dynwind-unwind-handler free left.start 0)
      (set left-pointer &left))
    (set left-pointer 0))
  (if (scm-is-pair scm-right)
    (begin
      (status-require (scm->db-ids scm-right &right))
      (scm-dynwind-unwind-handler free right.start 0)
      (set right-pointer &right))
    (set right-pointer 0))
  (if (scm-is-pair scm-right)
    (begin
      (status-require (scm->db-ids scm-label &label))
      (scm-dynwind-unwind-handler free right.start 0)
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
  (sc-comment "retrieve")
  (if (scm-is-symbol scm-retrieve)
    (case scm-is-eq scm-retrieve
      (scm-symbol-right (set scm-from-relations scm-from-db-relations-retrieve-right))
      (scm-symbol-left (set scm-from-relations scm-from-db-relations-retrieve-left))
      (scm-symbol-label (set scm-from-relations scm-from-db-relations-retrieve-label))
      (scm-symbol-ordinal (set scm-from-relations scm-from-db-relations-retrieve-ordinal))
      (else (status-set-both-goto db-status-group-db-guile status-id-invalid-argument)))
    (set scm-from-relations scm-from-db-relations))
  (sc-comment "db-relation-select")
  (status-require (db-helper-malloc (sizeof db-guile-relation-selection-t) &selection))
  (scm-dynwind-unwind-handler free selection 0)
  (status-require
    (db-relation-select
      (pointer-get (scm->db-txn scm-txn))
      left-pointer right-pointer label-pointer ordinal-pointer &selection:selection))
  (set
    selection:left left
    selection:right right
    selection:label label
    selection:scm-from-relations scm-from-relations)
  (set scm-selection (scm-from-db-selection selection))
  (db-guile-selection-register selection db-guile-selection-type-relation)
  (label exit
    (scm-dynwind-end)
    (scm-from-status-return scm-selection)))

(define (scm-db-record-select scm-txn scm-type scm-matcher scm-matcher-state) (SCM SCM SCM SCM SCM)
  status-declare
  (declare
    matcher db-record-matcher-t
    matcher-state void*
    scm-selection SCM
    scm-state SCM
    selection db-record-selection-t*)
  (sc-comment "matcher")
  (if (scm-is-true (scm-procedure? scm-matcher))
    (set
      scm-state
      (scm-cons
        scm-matcher
        (if* (scm-is-true (scm-list? scm-matcher-state)) scm-matcher-state
          (scm-list-1 scm-matcher-state)))
      matcher-state &scm-state
      matcher db-guile-record-matcher)
    (set
      matcher 0
      matcher-state 0))
  (sc-comment "record-select")
  (set selection (scm-gc-calloc (sizeof db-record-selection-t) "record-selection"))
  (scm-dynwind-unwind-handler free selection 0)
  (status-require
    (db-record-select
      (pointer-get (scm->db-txn scm-txn)) (scm->db-type scm-type) matcher matcher-state selection))
  (set scm-selection (scm-from-db-selection selection))
  (db-guile-selection-register selection db-guile-selection-type-relation)
  (label exit
    (scm-from-status-return scm-selection)))

(define (scm-db-record-ref scm-type scm-record scm-field) (SCM SCM SCM SCM)
  (declare
    value db-record-value-t
    type db-type-t*
    field-offset db-fields-len-t)
  (set
    field-offset (scm->uint scm-field)
    value
    (db-record-ref (scm->db-type scm-type) (pointer-get (scm->db-record scm-record)) field-offset))
  (return (scm-from-field-data value (: (+ field-offset type:fields) type))))

(define (scm-db-record-read scm-selection scm-count) (SCM SCM SCM)
  status-declare
  (declare
    records db-records-t
    count db-count-t
    selection db-guile-selection-t*
    result SCM)
  (set
    result SCM-UNSPECIFIED
    count (scm->uint scm-count)
    selection (scm->db-selection scm-selection))
  (scm-dynwind-begin 0)
  (status-require (db-records-new count &records))
  (scm-dynwind-free records.start)
  (status-require
    (db-record-read
      (pointer-get (convert-type selection:selection db-record-selection-t*)) count &records))
  (set result (scm-from-db-records records))
  (label exit
    (scm-dynwind-end)
    (scm-from-status-return result)))

(define (scm-db-relation-read scm-selection scm-count) (SCM SCM SCM)
  status-declare
  (declare
    relations db-relations-t
    count db-count-t
    selection db-guile-selection-t*
    relation-selection db-guile-relation-selection-t*
    result SCM)
  (set
    result SCM-UNSPECIFIED
    count (scm->uint scm-count)
    selection (scm->db-selection scm-selection)
    relation-selection (convert-type selection:selection db-guile-relation-selection-t*))
  (scm-dynwind-begin 0)
  (status-require (db-relations-new count &relations))
  (scm-dynwind-free relations.start)
  (status-require (db-relation-read &relation-selection:selection count &relations))
  (set result (relation-selection:scm-from-relations relations))
  (label exit
    (scm-dynwind-end)
    (scm-from-status-return result)))

(define (scm-db-index-select scm-txn) (SCM SCM))
(define (scm-db-index-read scm-txn) (SCM SCM))
(define (scm-db-record-index-select scm-txn) (SCM SCM))
(define (scm-db-record-index-read scm-txn) (SCM SCM))
(define (scm-db-record-virtual scm-data) (SCM SCM))
; db-record->record

(define (db-guile-init) void
  "prepare scm valuaes and register guile bindings"
  (declare
    type-slots SCM
    scm-symbol-data SCM)
  (set
    scm-rnrs-raise (scm-c-public-ref "rnrs exceptions" "raise")
    scm-symbol-binary (scm-from-latin1-symbol "binary")
    scm-symbol-binary128 (scm-from-latin1-symbol "binary128")
    scm-symbol-binary16 (scm-from-latin1-symbol "binary16")
    scm-symbol-binary256 (scm-from-latin1-symbol "binary256")
    scm-symbol-binary32 (scm-from-latin1-symbol "binary32")
    scm-symbol-binary512 (scm-from-latin1-symbol "binary512")
    scm-symbol-binary64 (scm-from-latin1-symbol "binary64")
    scm-symbol-binary8 (scm-from-latin1-symbol "binary8")
    scm-symbol-data (scm-from-latin1-symbol "data")
    scm-symbol-float32 (scm-from-latin1-symbol "float32")
    scm-symbol-float64 (scm-from-latin1-symbol "float64")
    scm-symbol-int128 (scm-from-latin1-symbol "int128")
    scm-symbol-int16 (scm-from-latin1-symbol "int16")
    scm-symbol-int256 (scm-from-latin1-symbol "int256")
    scm-symbol-int32 (scm-from-latin1-symbol "int32")
    scm-symbol-int512 (scm-from-latin1-symbol "int512")
    scm-symbol-int64 (scm-from-latin1-symbol "int64")
    scm-symbol-int8 (scm-from-latin1-symbol "int8")
    scm-symbol-label (scm-from-latin1-symbol "label")
    scm-symbol-left (scm-from-latin1-symbol "left")
    scm-symbol-max (scm-from-latin1-symbol "max")
    scm-symbol-min (scm-from-latin1-symbol "min")
    scm-symbol-ordinal (scm-from-latin1-symbol "ordinal")
    scm-symbol-right (scm-from-latin1-symbol "right")
    scm-symbol-string (scm-from-latin1-symbol "string")
    scm-symbol-string128 (scm-from-latin1-symbol "string128")
    scm-symbol-string16 (scm-from-latin1-symbol "string16")
    scm-symbol-string256 (scm-from-latin1-symbol "string256")
    scm-symbol-string32 (scm-from-latin1-symbol "string32")
    scm-symbol-string512 (scm-from-latin1-symbol "string512")
    scm-symbol-string64 (scm-from-latin1-symbol "string64")
    scm-symbol-string8 (scm-from-latin1-symbol "string8")
    scm-symbol-uint128 (scm-from-latin1-symbol "uint128")
    scm-symbol-uint16 (scm-from-latin1-symbol "uint16")
    scm-symbol-uint256 (scm-from-latin1-symbol "uint256")
    scm-symbol-uint32 (scm-from-latin1-symbol "uint32")
    scm-symbol-uint512 (scm-from-latin1-symbol "uint512")
    scm-symbol-uint64 (scm-from-latin1-symbol "uint64")
    scm-symbol-uint8 (scm-from-latin1-symbol "uint8")
    type-slots (scm-list-1 scm-symbol-data)
    scm-type-env (scm-make-foreign-object-type (scm-from-latin1-symbol "db-env") type-slots 0)
    scm-type-txn (scm-make-foreign-object-type (scm-from-latin1-symbol "db-txn") type-slots 0)
    scm-type-record (scm-make-foreign-object-type (scm-from-latin1-symbol "db-record") type-slots 0)
    scm-type-selection
    (scm-make-foreign-object-type (scm-from-latin1-symbol "db-selection") type-slots 0) type-slots
    (scm-list-2 scm-symbol-data (scm-from-latin1-symbol "env")) scm-type-type
    (scm-make-foreign-object-type (scm-from-latin1-symbol "db-type") type-slots 0) scm-type-index
    (scm-make-foreign-object-type (scm-from-latin1-symbol "db-index") type-slots 0))
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
  (scm-c-define-procedure-c "db-type-delete" 2 0 0 scm-db-type-delete "")
  (scm-c-define-procedure-c "db-type-get" 2 0 0 scm-db-type-get "")
  (scm-c-define-procedure-c "db-type-id" 1 0 0 scm-db-type-id "")
  (scm-c-define-procedure-c "db-type-name" 1 0 0 scm-db-type-name "")
  (scm-c-define-procedure-c "db-type-indices" 1 0 0 scm-db-type-indices "")
  (scm-c-define-procedure-c "db-type-fields" 1 0 0 scm-db-type-fields "")
  (scm-c-define-procedure-c "db-type-virtual?" 1 0 0 scm-db-type-virtual? "")
  (scm-c-define-procedure-c "db-type-flags" 1 0 0 scm-db-type-flags "")
  (scm-c-define-procedure-c "db-index-create" 3 0 0 scm-db-index-create "")
  (scm-c-define-procedure-c "db-index-delete" 2 0 0 scm-db-index-delete "env index -> unspecified")
  (scm-c-define-procedure-c
    "db-index-get" 3 0 0 scm-db-index-get "env type fields:(integer:offset ...) -> index")
  (scm-c-define-procedure-c
    "db-index-rebuild" 2 0 0 scm-db-index-rebuild "env index -> unspecified")
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
    scm-db-relation-ensure "db-txn list:left list:right list:label [ordinal-generator ordinal-state]")
  (scm-c-define-procedure-c
    "db-relation-select"
    1 4 0 scm-db-relation-select "db-txn [list:left list:right list:label ordinal-generator]")
  (scm-c-define-procedure-c "db-relation-read" 2 0 0 scm-db-relation-read "selection integer:count")
  (scm-c-define-procedure-c
    "db-record-select" 2 2 0 scm-db-record-select "txn type [matcher matcher-state]")
  (scm-c-define-procedure-c "db-record-read" 2 0 0 scm-db-record-read "selection integer:count")
  (scm-c-define-procedure-c "db-record-ref" 3 0 0 scm-db-record-ref "type record field:integer"))