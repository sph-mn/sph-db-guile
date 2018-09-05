(sc-comment
  "bindings that arent part of the exported scheme api and debug features."
  "separate file because it is easier to start from the exported features")

(enum (status-id-field-name-not-found status-id-field-value-invalid))

(pre-define
  db-status-group-db-guile db-status-group-last
  (scm-options-get options name result)
  (begin
    "SCM uint8_t* SCM -> unspecified"
    (set
      result (scm-assoc-ref scm-options (scm-from-latin1-symbol name))
      result
      (if* (scm-is-pair result) (scm-tail result)
        SCM-UNDEFINED)))
  ; scm types
  (db-env->scm pointer) (scm-make-foreign-object-1 scm-type-env pointer)
  (db-txn->scm pointer) (scm-make-foreign-object-1 scm-type-txn pointer)
  (db-index->scm pointer env) (scm-make-foreign-object-2 scm-type-index pointer env)
  (db-type->scm pointer env) (scm-make-foreign-object-2 scm-type-type pointer env)
  (db-selection->scm pointer) (scm-make-foreign-object-1 scm-type-selection pointer)
  (scm->db-env a) (convert-type (scm-foreign-object-ref a 0) db-env-t*)
  (scm->db-txn a) (convert-type (scm-foreign-object-ref a 0) db-txn-t*)
  (scm->db-index a) (convert-type (scm-foreign-object-ref a 0) db-index-t*)
  (scm->db-type a) (convert-type (scm-foreign-object-ref a 0) db-type-t*)
  (scm->db-selection a selection-name)
  (convert-type (scm-foreign-object-ref a 0) (pre-concat db_ selection-name _selection-t*))
  (scm-type->db-env a) (convert-type (scm-foreign-object-ref a 1) db-env-t*)
  (scm-index->db-env a) (convert-type (scm-foreign-object-ref a 1) db-env-t*)
  ; error handling
  (status->scm-error a) (scm-c-error (db-guile-status-name a) (db-guile-status-description a))
  (scm-c-error name description)
  (scm-call-1
    scm-rnrs-raise
    (scm-list-3
      (scm-from-latin1-symbol name)
      (scm-cons (scm-from-latin1-symbol "description") (scm-from-utf8-string description))
      (scm-cons (scm-from-latin1-symbol "c-routine") (scm-from-latin1-symbol __FUNCTION__))))
  (status->scm-return result) (return (status->scm result))
  (status->scm result)
  (if* status-is-success result
    (status->scm-error status)))

(declare
  scm-type-env SCM
  scm-type-txn SCM
  scm-type-selection SCM
  scm-type-type SCM
  scm-type-index SCM
  scm-rnrs-raise SCM
  scm-symbol-binary SCM
  scm-symbol-string SCM
  scm-symbol-float32 SCM
  scm-symbol-float64 SCM
  scm-symbol-int8 SCM
  scm-symbol-int16 SCM
  scm-symbol-int32 SCM
  scm-symbol-int64 SCM
  scm-symbol-uint8 SCM
  scm-symbol-uint16 SCM
  scm-symbol-uint32 SCM
  scm-symbol-uint64 SCM
  scm-symbol-string8 SCM
  scm-symbol-string16 SCM
  scm-symbol-string32 SCM
  scm-symbol-string64 SCM)

(define (scm->field-offset scm-a type result) (status-t SCM db-type-t* db-fields-len-t*)
  "get the db-field for either a field offset integer or field name"
  status-declare
  (declare
    field db-field-t*
    field-name uint8-t*)
  (set field-name 0)
  (if (scm-is-integer scm-a) (set *result (scm->uint scm-a))
    (begin
      (set
        field-name (scm->utf8-stringn scm-a 0)
        field (db-type-field-get type field-name))
      (free field-name)
      (if field (set *result field:index)
        (status-set-both-goto db-status-group-db-guile status-id-field-name-not-found))))
  (label exit
    (return status)))

(define (scm->field-offsets scm-type scm-fields result result-len)
  (status-t SCM SCM db-fields-len-t** db-fields-len-t*)
  "memory for result is handled by gc"
  status-declare
  (declare
    scm-field SCM
    field db-field-t*
    i db-fields-len-t
    fields-len db-fields-len-t
    fields db-fields-len-t*
    field-name uint8-t*
    type db-type-t*)
  (set
    fields-len (scm->uint (scm-length scm-fields))
    type (scm->db-type scm-type)
    fields (scm-gc-calloc (* fields-len (sizeof db-fields-len-t)) "fields"))
  (for
    ( (set i 0) (< i fields-len)
      (set
        i (+ 1 i)
        scm-fields (scm-tail scm-fields)))
    (set scm-field (scm-first scm-fields))
    (status-require (scm->field-offset (scm-first scm-fields) type (+ i fields))))
  (set
    *result fields
    *result-len fields-len)
  (label exit
    (return status)))

(define (db-guile-status-description a) (uint8-t* status-t)
  "get the description if available for a status"
  (declare b char*)
  (case = a.group
    (db-status-group-db-guile
      (case = a.id
        (status-id-field-name-not-found (set b "no field found with given name"))
        (else (set b ""))))
    (else (set b (db-status-description a))))
  (return (convert-type b uint8-t*)))

(define (db-guile-status-name a) (uint8-t* status-t)
  "get the name if available for a status"
  (declare b char*)
  (case = a.group
    (db-status-group-db-guile
      (case = a.id
        (status-id-field-name-not-found (set b "field-not-found"))
        (else (set b ""))))
    (else (set b (db-status-name a))))
  (return (convert-type b uint8-t*)))

(define (scm->db-field-type a) (db-field-type-t SCM)
  "float32 not supported by guile"
  (case scm-is-eq a
    (scm-symbol-binary (return 1))
    (scm-symbol-string (return 3))
    (scm-symbol-float64 (return 6))
    (scm-symbol-int16 (return 80))
    (scm-symbol-int32 (return 112))
    (scm-symbol-int64 (return 144))
    (scm-symbol-int8 (return 48))
    (scm-symbol-uint8 (return 32))
    (scm-symbol-uint16 (return 64))
    (scm-symbol-uint32 (return 96))
    (scm-symbol-uint64 (return 128))
    (scm-symbol-string8 (return 34))
    (scm-symbol-string16 (return 66))
    (scm-symbol-string32 (return 98))
    (scm-symbol-string64 (return 130))
    (else (return 0))))

(define (db-field-type->scm a) (SCM db-field-type-t)
  (case = a
    (1 (return scm-symbol-binary))
    (3 (return scm-symbol-string))
    (4 (return scm-symbol-float32))
    (6 (return scm-symbol-float64))
    (80 (return scm-symbol-int16))
    (112 (return scm-symbol-int32))
    (144 (return scm-symbol-int64))
    (48 (return scm-symbol-int8))
    (32 (return scm-symbol-uint8))
    (64 (return scm-symbol-uint16))
    (96 (return scm-symbol-uint32))
    (128 (return scm-symbol-uint64))
    (34 (return scm-symbol-string8))
    (66 (return scm-symbol-string16))
    (98 (return scm-symbol-string32))
    (130 (return scm-symbol-string64))
    (else (return SCM-BOOL-F))))

(define (scm-from-mdb-stat a) (SCM MDB-stat)
  "-> ((key . value) ...)"
  (declare b SCM)
  (set
    b SCM-EOL
    b (scm-acons (scm-from-latin1-symbol "ms-entries") (scm-from-uint a.ms-entries) b)
    b (scm-acons (scm-from-latin1-symbol "ms-psize") (scm-from-uint a.ms-psize) b)
    b (scm-acons (scm-from-latin1-symbol "ms-depth") (scm-from-uint a.ms-depth) b)
    b (scm-acons (scm-from-latin1-symbol "ms-branch-pages") (scm-from-uint a.ms-branch-pages) b)
    b (scm-acons (scm-from-latin1-symbol "ms-leaf-pages") (scm-from-uint a.ms-leaf-pages) b)
    b (scm-acons (scm-from-latin1-symbol "ms-overflow-pages") (scm-from-uint a.ms-overflow-pages) b))
  (return b))

(define (db-index->scm-fields a) (SCM db-index-t*)
  (declare
    field db-field-t
    type db-type-t*
    i db-fields-len-t
    result SCM)
  (set
    result SCM-EOL
    type a:type)
  (for ((set i 0) (< i a:fields-len) (set i (+ 1 i)))
    (set
      field (array-get type:fields (array-get a:fields i))
      result
      (scm-cons
        (scm-cons
          (scm-from-uint (array-get a:fields i)) (scm-from-utf8-stringn field.name field.name-len))
        result)))
  (return result))

(define (scm->field-data scm-a field-type result-data result-size result-is-ref)
  (status-t SCM db-field-type-t void** size-t* boolean*)
  "result-data has to be freed by the caller only if result-is-ref is true"
  status-declare
  (declare
    size size-t
    data void*)
  (scm-dynwind-begin 0)
  (cond
    ( (scm-is-bytevector scm-a)
      (if (not (= db-field-type-binary field-type))
        (status-set-id-goto status-id-field-value-invalid))
      (set
        *result-is-ref #t
        *result-data (SCM-BYTEVECTOR-CONTENTS scm-a)
        *result-size (SCM-BYTEVECTOR-LENGTH scm-a)))
    ( (scm-is-string scm-a)
      (set size (scm-c-string-utf8-length scm-a))
      (case = field-type
        (db-field-type-string #t)
        (db-field-type-string64 (if (< 8 size) (status-set-id-goto status-id-field-value-invalid)))
        (db-field-type-string32 (if (< 4 size) (status-set-id-goto status-id-field-value-invalid)))
        (db-field-type-string16 (if (< 2 size) (status-set-id-goto status-id-field-value-invalid)))
        (db-field-type-string8 (if (< 1 size) (status-set-id-goto status-id-field-value-invalid)))
        (else (status-set-id-goto status-id-field-value-invalid)))
      (set
        *result-is-ref #f
        *result-data (scm->utf8-stringn scm-a 0)
        *result-size size))
    ( (scm-is-integer scm-a)
      (db-malloc data 8)
      (scm-dynwind-unwind-handler free data 0)
      (case = field-type
        (db-field-type-uint64
          (set
            (pointer-get (convert-type data uint64-t*)) (scm->uint64 scm-a)
            size 8))
        (db-field-type-uint32
          (set
            (pointer-get (convert-type data uint32-t*)) (scm->uint32 scm-a)
            size 4))
        (db-field-type-uint16
          (set
            (pointer-get (convert-type data uint16-t*)) (scm->uint16 scm-a)
            size 2))
        (db-field-type-uint8
          (set
            (pointer-get (convert-type data uint16-t*)) (scm->uint8 scm-a)
            size 1))
        (db-field-type-int64
          (set
            (pointer-get (convert-type data int64-t*)) (scm->int64 scm-a)
            size 8))
        (db-field-type-int32
          (set
            (pointer-get (convert-type data int32-t*)) (scm->int32 scm-a)
            size 4))
        (db-field-type-int16
          (set
            (pointer-get (convert-type data int16-t*)) (scm->int16 scm-a)
            size 2))
        (db-field-type-int8
          (set
            (pointer-get (convert-type data int8-t*)) (scm->int8 scm-a)
            size 1))
        (else (status-set-id-goto status-id-field-value-invalid)))
      (set
        *result-is-ref #f
        *result-data data
        *result-size size))
    ( (scm-is-rational scm-a)
      (db-malloc data 8)
      (scm-dynwind-unwind-handler free data 0)
      (case = field-type
        (db-field-type-float64
          (set
            (pointer-get (convert-type data double*)) (scm->double scm-a)
            size 8))
        (else
          (sc-comment "for some reason there is no scm->float")
          (status-set-id-goto status-id-field-value-invalid)))
      (set
        *result-is-ref #f
        *result-data data
        *result-size size))
    (else (status-set-id-goto status-id-field-value-invalid)))
  (label exit
    (scm-dynwind-end)
    (return status)))

#;(define (db-ids->scm a) (SCM db-ids-t)
  (define b SCM SCM-EOL)
  (while (db-ids-in-range a)
    (set b (scm-cons (scm-from-uint (db-ids-first a)) b))
    (db-ids-forward a))
  (return result))

#;(define (scm->db-ids scm-a result) (status-t SCM db-ids-t*)
  "result is allocated by this routine and caller frees"
  status-declare
  (declare b db-ids-t)
  (set b *result)
  (while (not (scm-is-null scm-a))
    (db-ids-add b (scm-from-uint (scm-first scm-a)))
    (if b
      (set
        *result b
        scm-a (scm-tail scm-a))
      (begin
        (db-ids-destroy *result)
        (db-status-set-id-goto db-status-id-memory))))
  (label exit
    (return status)))

#;(define (db-relations->scm a convert-data)
  (SCM db-data-records-t* (function-pointer SCM db-data-record-t))
  (define result SCM SCM-EOL)
  (define record db-data-record-t)
  (define data SCM)
  (while a
    (set
      record (db-data-records-first a)
      data
      (if* record.size (convert-data record)
        scm-bytevector-null)
      result (scm-cons (scm-vector (scm-list-2 (db-id->scm record.id) data)) result)
      a (db-data-records-rest a)))
  (return result))

#;(define (db-relations->scm a) (SCM db-relations-t*)
  (define result SCM SCM-EOL)
  (define record db-relation-record-t)
  (while a
    (set
      record (db-relations-first a)
      result
      (scm-cons
        (scm-vector
          (scm-list-4
            (db-id->scm (struct-get record left))
            (db-id->scm (struct-get record right))
            (db-id->scm (struct-get record label)) (db-id->scm (struct-get record ordinal))))
        result)
      a (db-relations-rest a)))
  (return result))

(pre-include "./selections.c")