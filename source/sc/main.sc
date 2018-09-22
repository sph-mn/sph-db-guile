(sc-comment
  "sph-db-guile registers scheme procedures that when called execute specific c-functions that manage calls to sph-db")

(pre-include
  "libguile.h"
  "sph-db.h"
  "sph-db-extra.h"
  "./foreign/sph/one.c"
  "./foreign/sph/guile.c" "./foreign/sph/memreg.c" "./foreign/sph/memreg-heap.c" "./helper.c")

(define (scm-db-txn-active? a) (SCM SCM) (return (scm-from-bool (scm->db-txn a))))
(define (scm-db-env-open? a) (SCM SCM) (return (scm-from-bool (: (scm->db-env a) is-open))))
(define (scm-db-env-root a) (SCM SCM) (return (scm-from-utf8-string (: (scm->db-env a) root))))

(define (scm-db-env-maxkeysize a) (SCM SCM)
  (return (scm-from-uint32 (: (scm->db-env a) maxkeysize))))

(define (scm-db-env-format a) (SCM SCM) (return (scm-from-uint32 (: (scm->db-env a) format))))
(define (scm-db-type-id a) (SCM SCM) (return (scm-from-uintmax (: (scm->db-type a) id))))
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
          (scm-from-utf8-stringn field.name (strlen field.name)) (scm-from-db-field-type field.type))
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
      (if (scm-is-integer a) (set options.maximum-reader-count (scm->uint32 a)))
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
    fields-len (scm->uintmax (scm-length scm-fields)))
  (status-require (db-helper-calloc (* fields-len (sizeof db-field-t)) &fields))
  (scm-dynwind-free fields)
  (for
    ( (set i 0) (< i fields-len)
      (set
        i (+ 1 i)
        scm-fields (scm-tail scm-fields)))
    (set scm-field (scm-first scm-fields))
    (if (scm-is-symbol scm-field)
      (set
        field-type (scm->db-field-type scm-field)
        field-name "")
      (begin
        (sc-comment "pair")
        (set
          field-name (scm->utf8-stringn (scm-first scm-field) 0)
          field-type (scm->db-field-type (scm-tail scm-field))
          field-name-len (strlen field-name))
        (scm-dynwind-free field-name)))
    (db-field-set (array-get fields i) field-type field-name))
  (status-require (db-type-create (scm->db-env scm-env) name fields fields-len flags &type))
  (label exit
    (scm-from-status-dynwind-end-return (scm-from-db-type type))))

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
    (set type (db-type-get-by-id (scm->db-env scm-env) (scm->uintmax scm-name-or-id))))
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
  (memreg-heap-declare allocations)
  (declare
    scm-value SCM
    result-id db-id-t
    type db-type-t*)
  (scm-dynwind-begin 0)
  (set type (scm->db-type scm-type))
  (status-require (scm-c->db-record-values type scm-values &values &allocations))
  (scm-dynwind-unwind-handler db-guile-memreg-heap-free &allocations SCM-F-WIND-EXPLICITLY)
  (status-require (db-record-create (pointer-get (scm->db-txn scm-txn)) values &result-id))
  (label exit
    (scm-from-status-dynwind-end-return (scm-from-uintmax result-id))))

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
  (sc-comment "ordinal setter. generated by procedure or fixed value")
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
        (scm->uintmax scm-ordinal-state))
      ordinal-state &ordinal-value))
  (db-relation-ensure
    (pointer-get (scm->db-txn scm-txn)) left right label ordinal-generator ordinal-state)
  (label exit
    (scm-from-status-return SCM-BOOL-T)))

(define (scm-db-id-type a) (SCM SCM) (scm-from-uintmax (db-id-type (scm->uintmax a))))
(define (scm-db-id-element a) (SCM SCM) (scm-from-uintmax (db-id-element (scm->uintmax a))))

(define (scm-db-id-add-type a type) (SCM SCM SCM)
  (scm-from-uintmax (db-id-add-type (scm->uintmax a) (scm->uintmax type))))

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
    (scm-from-status-dynwind-end-return result)))

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
  (memreg-init 5)
  (if (or (scm-is-null scm-left) (scm-is-null scm-right) (scm-is-null scm-label))
    (return (scm-from-db-selection 0)))
  (set selection 0)
  (scm-dynwind-begin 0)
  (sc-comment "left/right/label")
  (if (scm-is-pair scm-left)
    (begin
      (status-require (scm->db-ids scm-left &left))
      (scm-dynwind-unwind-handler free left.start 0)
      (memreg-add left.start)
      (set left-pointer &left))
    (set left-pointer 0))
  (if (scm-is-pair scm-right)
    (begin
      (status-require (scm->db-ids scm-right &right))
      (scm-dynwind-unwind-handler free right.start 0)
      (memreg-add right.start)
      (set right-pointer &right))
    (set right-pointer 0))
  (if (scm-is-pair scm-right)
    (begin
      (status-require (scm->db-ids scm-label &label))
      (scm-dynwind-unwind-handler free label.start 0)
      (memreg-add label.start)
      (set label-pointer &label))
    (set label-pointer 0))
  (sc-comment "ordinal")
  (cond
    ( (scm-is-true (scm-list? scm-ordinal))
      (set
        scm-ordinal-min (scm-assoc-ref scm-ordinal scm-symbol-min)
        scm-ordinal-max (scm-assoc-ref scm-ordinal scm-symbol-max)
        ordinal.min
        (if* (scm-is-integer scm-ordinal-min) (scm->uintmax scm-ordinal-min)
          0)
        ordinal.max
        (if* (scm-is-integer scm-ordinal-max) (scm->uintmax scm-ordinal-max)
          0)
        ordinal-pointer &ordinal))
    ( (scm-is-integer scm-ordinal)
      (set
        ordinal.min (scm->uintmax scm-ordinal)
        ordinal.max ordinal.min
        ordinal-pointer &ordinal))
    (else (set ordinal-pointer 0)))
  (sc-comment "retrieve")
  (if (scm-is-symbol scm-retrieve)
    (case scm-is-eq scm-retrieve
      (scm-symbol-right (set scm-from-relations scm-from-db-relations-retrieve-right))
      (scm-symbol-left (set scm-from-relations scm-from-db-relations-retrieve-left))
      (scm-symbol-label (set scm-from-relations scm-from-db-relations-retrieve-label))
      (scm-symbol-ordinal (set scm-from-relations scm-from-db-relations-retrieve-ordinal))
      (else (status-set-both-goto status-group-db-guile status-id-invalid-argument)))
    (set scm-from-relations scm-from-db-relations))
  (sc-comment "db-relation-select")
  (status-require (db-helper-malloc (sizeof db-guile-relation-selection-t) &selection))
  (scm-dynwind-unwind-handler free selection 0)
  (memreg-add selection)
  (status-require-read
    (db-relation-select
      (pointer-get (scm->db-txn scm-txn))
      left-pointer right-pointer label-pointer ordinal-pointer &selection:selection))
  (set
    selection:left left
    selection:right right
    selection:label label
    selection:scm-from-relations scm-from-relations
    selection:status-id status.id
    scm-selection (scm-from-db-selection selection))
  db-status-success-if-notfound
  (db-guile-selection-register selection db-guile-selection-type-relation)
  (label exit
    (if status-is-failure memreg-free)
    (scm-from-status-dynwind-end-return scm-selection)))

(define (scm-db-record-select scm-txn scm-type scm-matcher scm-matcher-state) (SCM SCM SCM SCM SCM)
  status-declare
  (declare
    matcher db-record-matcher-t
    matcher-state void*
    scm-selection SCM
    scm-state SCM
    selection db-guile-record-selection-t*)
  (scm-dynwind-begin 0)
  (status-require (db-helper-malloc (sizeof db-guile-record-selection-t) &selection))
  (scm-dynwind-unwind-handler free selection 0)
  (sc-comment "matcher")
  (if (scm-is-true (scm-procedure? scm-matcher))
    (set
      selection:matcher
      (scm-cons
        scm-matcher
        (if* (scm-is-undefined scm-matcher-state) SCM-EOL
          (if* (scm-is-true (scm-list? scm-matcher-state)) scm-matcher-state
            (scm-list-1 scm-matcher-state))))
      matcher-state &selection:matcher
      matcher db-guile-record-matcher)
    (set
      matcher 0
      matcher-state 0))
  (sc-comment "record-select")
  (status-require-read
    (db-record-select
      (pointer-get (scm->db-txn scm-txn))
      (scm->db-type scm-type) matcher matcher-state &selection:selection))
  (set
    selection:status-id status.id
    scm-selection (scm-from-db-selection selection))
  (db-guile-selection-register selection db-guile-selection-type-record)
  (label exit
    (scm-from-status-dynwind-end-return scm-selection)))

(define (scm-db-record-ref scm-type scm-record scm-field) (SCM SCM SCM SCM)
  (declare
    value db-record-value-t
    type db-type-t*
    field-offset db-fields-len-t)
  (set
    type (scm->db-type scm-type)
    field-offset (scm->uintmax scm-field)
    value (db-record-ref type (pointer-get (scm->db-record scm-record)) field-offset))
  (return (scm-from-field-data value.data value.size (: (+ field-offset type:fields) type))))

(define (scm-db-record->vector scm-type scm-record) (SCM SCM SCM)
  (declare
    fields-len db-fields-len-t
    i db-fields-len-t
    result SCM
    type db-type-t*
    value db-record-value-t)
  (set
    type (scm->db-type scm-type)
    fields-len type:fields-len
    result (scm-c-make-vector fields-len SCM-BOOL-F))
  (for ((set i 0) (< i fields-len) (set i (+ 1 i)))
    (set value (db-record-ref type (pointer-get (scm->db-record scm-record)) i))
    (scm-c-vector-set!
      result i (scm-from-field-data value.data value.size (: (+ i type:fields) type))))
  (return result))

(define (scm-db-record-read scm-selection scm-count) (SCM SCM SCM)
  "allow multiple calls by tracking the record-select return status and
  eventually not calling record-select again"
  status-declare
  (declare
    records db-records-t
    count db-count-t
    result SCM
    selection db-guile-record-selection-t*)
  (set
    result SCM-EOL
    selection (convert-type (scm->db-selection scm-selection) db-guile-record-selection-t*))
  (if (not (= status-id-success selection:status-id)) (return result))
  (set count (scm->uintmax scm-count))
  (scm-dynwind-begin 0)
  (status-require (db-records-new count &records))
  (scm-dynwind-free records.start)
  (status-require-read (db-record-read selection:selection count &records))
  (set
    selection:status-id status.id
    result (scm-from-db-records records))
  (label exit
    db-status-success-if-notfound
    (scm-from-status-dynwind-end-return result)))

(define (scm-db-relation-read scm-selection scm-count) (SCM SCM SCM)
  status-declare
  (declare
    relations db-relations-t
    count db-count-t
    selection db-guile-relation-selection-t*
    result SCM)
  (set
    result SCM-EOL
    selection (scm->db-selection scm-selection))
  (if (not (= status-id-success selection:status-id)) (return result))
  (set count (scm->uintmax scm-count))
  (scm-dynwind-begin 0)
  (status-require (db-relations-new count &relations))
  (scm-dynwind-free relations.start)
  (status-require-read (db-relation-read &selection:selection count &relations))
  (set
    selection:status-id status.id
    result (selection:scm-from-relations relations))
  (label exit
    db-status-success-if-notfound
    (scm-from-status-dynwind-end-return result)))

(define (scm-db-record-update scm-txn scm-type scm-id scm-values) (SCM SCM SCM SCM SCM)
  status-declare
  (db-record-values-declare values)
  (declare
    type db-type-t*
    allocations memreg-register-t)
  (scm-dynwind-begin 0)
  (set type (scm->db-type scm-type))
  (status-require (scm-c->db-record-values type scm-values &values &allocations))
  (scm-dynwind-unwind-handler db-guile-memreg-heap-free &allocations SCM-F-WIND-EXPLICITLY)
  (status-require
    (db-record-update (pointer-get (scm->db-txn scm-txn)) (scm->uintmax scm-id) values))
  (label exit
    (scm-from-status-dynwind-end-return SCM-UNSPECIFIED)))

(define (scm-db-record-virtual scm-type scm-data) (SCM SCM SCM)
  status-declare
  (declare
    field-data void*
    field-data-needs-free boolean
    field-data-size size-t
    field-type db-field-type-t
    type db-type-t*
    id db-id-t)
  (set
    type (scm->db-type scm-type)
    field-type (struct-get (pointer-get type:fields) type))
  (sc-comment "assumes that no virtual types with invalid field sizes can be created")
  (status-require
    (scm->field-data scm-data field-type &field-data &field-data-size &field-data-needs-free))
  (set id (db-record-virtual type:id field-data field-data-size))
  (if field-data-needs-free (free field-data))
  (label exit
    (scm-from-status-return (scm-from-uintmax id))))

(define (scm-db-record-virtual-data scm-type scm-id) (SCM SCM SCM)
  status-declare
  (declare
    data void*
    size size-t
    id db-id-t
    field-type db-field-type-t
    type db-type-t*
    result SCM)
  (set
    result SCM-BOOL-F
    id (scm->uintmax scm-id)
    type (scm->db-type scm-type)
    size type:fields:size)
  (status-require (db-helper-malloc size &data))
  (set
    data (db-record-virtual-data id data size)
    result (scm-from-field-data data size type:fields:type))
  (label exit
    (scm-from-status-return result)))

#;(define (scm-db-index-select scm-txn scm-index scm-values) (SCM SCM SCM SCM)
  status-declare
  (declare selection db-guile-index-selection-t*)
  (scm-dynwind-begin 0)
  (status-require (db-helper-malloc (sizeof db-guile-index-selection-t) &selection))
  (scm-dynwind-unwind-handler free selection 0)
  (status-require
    (db-index-select
      (pointer-get (scm->db-txn scm-txn)) (scm->db-index scm-index) scm-values &selection:selection))
  (label exit
    (scm-from-status-dynwind-end-return SCM-UNSPECIFIED)))

(define (scm-db-index-read scm-txn) (SCM SCM))
(define (scm-db-record-index-select scm-txn) (SCM SCM))
(define (scm-db-record-index-read scm-txn scm-count) (SCM SCM SCM))

(define (db-guile-init) void
  "prepare scm values and register guile bindings"
  (declare
    type-slots SCM
    scm-symbol-data SCM)
  (set
    scm-rnrs-raise (scm-c-public-ref "rnrs exceptions" "raise")
    scm-symbol-binary8 (scm-from-latin1-symbol "binary8")
    scm-symbol-binary16 (scm-from-latin1-symbol "binary16")
    scm-symbol-binary32 (scm-from-latin1-symbol "binary32")
    scm-symbol-binary64 (scm-from-latin1-symbol "binary64")
    scm-symbol-string8 (scm-from-latin1-symbol "string8")
    scm-symbol-string16 (scm-from-latin1-symbol "string16")
    scm-symbol-string32 (scm-from-latin1-symbol "string32")
    scm-symbol-string64 (scm-from-latin1-symbol "string64")
    scm-symbol-binary128f (scm-from-latin1-symbol "binary128f")
    scm-symbol-binary16f (scm-from-latin1-symbol "binary16f")
    scm-symbol-binary256f (scm-from-latin1-symbol "binary256f")
    scm-symbol-binary32f (scm-from-latin1-symbol "binary32f")
    scm-symbol-binary64f (scm-from-latin1-symbol "binary64f")
    scm-symbol-binary8f (scm-from-latin1-symbol "binary8f")
    scm-symbol-data (scm-from-latin1-symbol "data")
    scm-symbol-float32f (scm-from-latin1-symbol "float32f")
    scm-symbol-float64f (scm-from-latin1-symbol "float64f")
    scm-symbol-int128f (scm-from-latin1-symbol "int128f")
    scm-symbol-int16f (scm-from-latin1-symbol "int16f")
    scm-symbol-int256f (scm-from-latin1-symbol "int256f")
    scm-symbol-int32f (scm-from-latin1-symbol "int32f")
    scm-symbol-int64f (scm-from-latin1-symbol "int64f")
    scm-symbol-int8f (scm-from-latin1-symbol "int8f")
    scm-symbol-label (scm-from-latin1-symbol "label")
    scm-symbol-left (scm-from-latin1-symbol "left")
    scm-symbol-max (scm-from-latin1-symbol "max")
    scm-symbol-min (scm-from-latin1-symbol "min")
    scm-symbol-ordinal (scm-from-latin1-symbol "ordinal")
    scm-symbol-right (scm-from-latin1-symbol "right")
    scm-symbol-string128f (scm-from-latin1-symbol "string128f")
    scm-symbol-string16f (scm-from-latin1-symbol "string16f")
    scm-symbol-string256f (scm-from-latin1-symbol "string256f")
    scm-symbol-string32f (scm-from-latin1-symbol "string32f")
    scm-symbol-string64f (scm-from-latin1-symbol "string64f")
    scm-symbol-string8f (scm-from-latin1-symbol "string8f")
    scm-symbol-uint128f (scm-from-latin1-symbol "uint128f")
    scm-symbol-uint16f (scm-from-latin1-symbol "uint16f")
    scm-symbol-uint256f (scm-from-latin1-symbol "uint256f")
    scm-symbol-uint32f (scm-from-latin1-symbol "uint32f")
    scm-symbol-uint64f (scm-from-latin1-symbol "uint64f")
    scm-symbol-uint8f (scm-from-latin1-symbol "uint8f")
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
  (scm-c-module-define m "db-type-flag-virtual" (scm-from-uint8 db-type-flag-virtual))
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
    1 5 0 scm-db-relation-select "db-txn [list:left list:right list:label retrieve ordinal]")
  (scm-c-define-procedure-c "db-relation-read" 2 0 0 scm-db-relation-read "selection integer:count")
  (scm-c-define-procedure-c
    "db-record-select" 2 2 0 scm-db-record-select "txn type [matcher matcher-state]")
  (scm-c-define-procedure-c "db-record-read" 2 0 0 scm-db-record-read "selection integer:count")
  (scm-c-define-procedure-c "db-record-ref" 3 0 0 scm-db-record-ref "type record field:integer")
  (scm-c-define-procedure-c
    "db-record-get" 2 0 0 scm-db-record-get "txn list:ids -> (db-record ...)")
  (scm-c-define-procedure-c
    "db-record->vector" 2 0 0 scm-db-record->vector "type db-record -> vector:#(any:value ...)")
  (scm-c-define-procedure-c
    "db-record-update"
    4 0 0 scm-db-record-update "db-txn db-type id list:((field-id . value) ...) -> unspecified")
  (scm-c-define-procedure-c "db-record-virtual" 2 0 0 scm-db-record-virtual "type data -> id")
  (scm-c-define-procedure-c
    "db-record-virtual-data" 2 0 0 scm-db-record-virtual-data "db-env id -> any:data"))