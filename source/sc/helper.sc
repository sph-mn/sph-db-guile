(sc-comment
  "bindings that arent part of the exported scheme api and debug features."
  "separate file because it is easier to start from the exported features")

(enum (status-id-field-name-not-found status-id-field-value-invalid status-id-invalid-argument))

(pre-define
  (scm-options-get options name result)
  (begin
    "SCM uint8_t* SCM -> unspecified
    get value for field with name from options alist and set result
    to it or undefined if it doesnt exist"
    (set
      result (scm-assoc-ref scm-options (scm-from-latin1-symbol name))
      result
      (if* (scm-is-pair result) (scm-tail result)
        SCM-UNDEFINED)))
  (define-scm-from-db-relations-retrieve field-name)
  (define ((pre-concat scm-from-db-relations-retrieve_ field-name) a) (SCM db-relations-t)
    (declare
      b SCM
      record db-relation-t)
    (set b SCM-EOL)
    (while (db-relations-in-range a)
      (set
        record (db-relations-get a)
        b (scm-cons (scm-from-uint record.field-name) b))
      (db-relations-forward a))
    (return b))
  ; scm types
  (scm-from-db-env pointer) (scm-make-foreign-object-1 scm-type-env pointer)
  (scm-from-db-txn pointer) (scm-make-foreign-object-1 scm-type-txn pointer)
  (scm-from-db-index pointer) (scm-make-foreign-object-1 scm-type-index pointer)
  (scm-from-db-type pointer) (scm-make-foreign-object-1 scm-type-type pointer)
  (scm-from-db-selection pointer) (scm-make-foreign-object-1 scm-type-selection pointer)
  (scm-from-db-record pointer) (scm-make-foreign-object-1 scm-type-record pointer)
  (scm->db-record a) (convert-type (scm-foreign-object-ref a 0) db-record-t*)
  (scm->db-env a) (convert-type (scm-foreign-object-ref a 0) db-env-t*)
  (scm->db-txn a) (convert-type (scm-foreign-object-ref a 0) db-txn-t*)
  (scm->db-index a) (convert-type (scm-foreign-object-ref a 0) db-index-t*)
  (scm->db-type a) (convert-type (scm-foreign-object-ref a 0) db-type-t*)
  (scm->db-selection a) (scm-foreign-object-ref a 0)
  ; error handling
  status-group-db-guile "db-guile"
  (scm-from-status-error a)
  (scm-c-error a.group (db-guile-status-name a) (db-guile-status-description a))
  (scm-c-error group name description)
  (scm-call-1
    scm-rnrs-raise
    (scm-list-4
      (scm-from-latin1-symbol group)
      (scm-from-latin1-symbol name)
      (scm-cons (scm-from-latin1-symbol "description") (scm-from-utf8-string description))
      (scm-cons (scm-from-latin1-symbol "c-routine") (scm-from-latin1-symbol __FUNCTION__))))
  (scm-from-status-return result) (return (scm-from-status result))
  (scm-from-status result)
  (if* status-is-success result
    (scm-from-status-error status)))

(declare
  scm-rnrs-raise SCM
  scm-symbol-binary8 SCM
  scm-symbol-binary16 SCM
  scm-symbol-binary32 SCM
  scm-symbol-binary64 SCM
  scm-symbol-string64 SCM
  scm-symbol-string32 SCM
  scm-symbol-string16 SCM
  scm-symbol-string8 SCM
  scm-symbol-binary8f SCM
  scm-symbol-binary16f SCM
  scm-symbol-binary32f SCM
  scm-symbol-binary64f SCM
  scm-symbol-float32f SCM
  scm-symbol-float64f SCM
  scm-symbol-int16f SCM
  scm-symbol-int32f SCM
  scm-symbol-int64f SCM
  scm-symbol-int8f SCM
  scm-symbol-min SCM
  scm-symbol-max SCM
  scm-symbol-label SCM
  scm-symbol-left SCM
  scm-symbol-ordinal SCM
  scm-symbol-right SCM
  scm-symbol-string16f SCM
  scm-symbol-string32f SCM
  scm-symbol-string64f SCM
  scm-symbol-string8f SCM
  scm-symbol-uint16f SCM
  scm-symbol-uint32f SCM
  scm-symbol-uint64f SCM
  scm-symbol-uint8f SCM
  scm-symbol-uint128f SCM
  scm-symbol-uint256f SCM
  scm-symbol-int128f SCM
  scm-symbol-int256f SCM
  scm-symbol-string128f SCM
  scm-symbol-string256f SCM
  scm-symbol-binary128f SCM
  scm-symbol-binary256f SCM
  scm-type-env SCM
  scm-type-index SCM
  scm-type-record SCM
  scm-type-selection SCM
  scm-type-txn SCM
  scm-type-type SCM)

(define-scm-from-db-relations-retrieve left)
(define-scm-from-db-relations-retrieve right)
(define-scm-from-db-relations-retrieve label)
(define-scm-from-db-relations-retrieve ordinal)

(define (scm->field-offset scm-a type result) (status-t SCM db-type-t* db-fields-len-t*)
  "get the db-field for either a field offset integer or field name"
  status-declare
  (declare
    field db-field-t*
    field-name uint8-t*)
  (set field-name 0)
  (if (scm-is-integer scm-a) (set *result (scm->uintmax scm-a))
    (begin
      (set
        field-name (scm->utf8-stringn scm-a 0)
        field (db-type-field-get type field-name))
      (free field-name)
      (if field (set *result field:offset)
        (status-set-both-goto status-group-db-guile status-id-field-name-not-found))))
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
    fields-len (scm->uintmax (scm-length scm-fields))
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
  (cond
    ( (not (strcmp status-group-db-guile a.group))
      (case = a.id
        (status-id-field-name-not-found (set b "no field found with given name"))
        (status-id-field-value-invalid (set b "field value invalid"))
        (status-id-invalid-argument (set b "invalid argument"))
        (else (set b ""))))
    (else (set b (db-status-description a))))
  (return (convert-type b uint8-t*)))

(define (db-guile-status-name a) (uint8-t* status-t)
  "get the name if available for a status"
  (declare b char*)
  (cond
    ( (not (strcmp status-group-db-guile a.group))
      (case = a.id
        (status-id-field-name-not-found (set b "field-not-found"))
        (status-id-field-value-invalid (set b "field-value-invalid"))
        (status-id-invalid-argument (set b "invalid-argument"))
        (else (set b "unknown"))))
    (else (set b (db-status-name a))))
  (return (convert-type b uint8-t*)))

(define (scm->db-field-type a) (db-field-type-t SCM)
  "float32 not supported by guile"
  (case scm-is-eq a
    (scm-symbol-string8 (return db-field-type-string8))
    (scm-symbol-string16 (return db-field-type-string16))
    (scm-symbol-string32 (return db-field-type-string32))
    (scm-symbol-string64 (return db-field-type-string64))
    (scm-symbol-binary8 (return db-field-type-binary8))
    (scm-symbol-binary16 (return db-field-type-binary16))
    (scm-symbol-binary32 (return db-field-type-binary32))
    (scm-symbol-binary64 (return db-field-type-binary64))
    (scm-symbol-binary8f (return db-field-type-binary8f))
    (scm-symbol-binary16f (return db-field-type-binary16f))
    (scm-symbol-binary32f (return db-field-type-binary32f))
    (scm-symbol-binary64f (return db-field-type-binary64f))
    (scm-symbol-binary128f (return db-field-type-binary128f))
    (scm-symbol-binary256f (return db-field-type-binary256f))
    (scm-symbol-uint8f (return db-field-type-uint8f))
    (scm-symbol-uint16f (return db-field-type-uint16f))
    (scm-symbol-uint32f (return db-field-type-uint32f))
    (scm-symbol-uint64f (return db-field-type-uint64f))
    (scm-symbol-uint128f (return db-field-type-uint128f))
    (scm-symbol-uint256f (return db-field-type-uint256f))
    (scm-symbol-int8f (return db-field-type-int8f))
    (scm-symbol-int16f (return db-field-type-int16f))
    (scm-symbol-int32f (return db-field-type-int32f))
    (scm-symbol-int64f (return db-field-type-int64f))
    (scm-symbol-float64f (return db-field-type-float64f))
    (scm-symbol-string8f (return db-field-type-string8f))
    (scm-symbol-string16f (return db-field-type-string16f))
    (scm-symbol-string32f (return db-field-type-string32f))
    (scm-symbol-string64f (return db-field-type-string64f))
    (scm-symbol-string128f (return db-field-type-string128f))
    (scm-symbol-string256f (return db-field-type-string256f))
    (scm-symbol-int128f (return db-field-type-int128f))
    (scm-symbol-int256f (return db-field-type-int256f))
    (else (return 0))))

(define (scm-from-db-field-type a) (SCM db-field-type-t)
  (case = a
    (db-field-type-string8 (return scm-symbol-string8))
    (db-field-type-string16 (return scm-symbol-string16))
    (db-field-type-string32 (return scm-symbol-string32))
    (db-field-type-string64 (return scm-symbol-string64))
    (db-field-type-binary8 (return scm-symbol-binary8))
    (db-field-type-binary16 (return scm-symbol-binary16))
    (db-field-type-binary32 (return scm-symbol-binary32))
    (db-field-type-binary64 (return scm-symbol-binary64))
    (db-field-type-binary8f (return scm-symbol-binary8f))
    (db-field-type-binary16f (return scm-symbol-binary16f))
    (db-field-type-binary32f (return scm-symbol-binary32f))
    (db-field-type-binary64f (return scm-symbol-binary64f))
    (db-field-type-uint8f (return scm-symbol-uint8f))
    (db-field-type-uint16f (return scm-symbol-uint16f))
    (db-field-type-uint32f (return scm-symbol-uint32f))
    (db-field-type-uint64f (return scm-symbol-uint64f))
    (db-field-type-int8f (return scm-symbol-int8f))
    (db-field-type-int16f (return scm-symbol-int16f))
    (db-field-type-int32f (return scm-symbol-int32f))
    (db-field-type-int64f (return scm-symbol-int64f))
    (db-field-type-string8f (return scm-symbol-string8f))
    (db-field-type-string16f (return scm-symbol-string16f))
    (db-field-type-string32f (return scm-symbol-string32f))
    (db-field-type-string64f (return scm-symbol-string64f))
    (db-field-type-float64f (return scm-symbol-float64f))
    (db-field-type-string128f (return scm-symbol-string128f))
    (db-field-type-string256f (return scm-symbol-string256f))
    (db-field-type-int128f (return scm-symbol-int128f))
    (db-field-type-int256f (return scm-symbol-int256f))
    (db-field-type-uint128f (return scm-symbol-uint128f))
    (db-field-type-uint256f (return scm-symbol-uint256f))
    (db-field-type-binary128f (return scm-symbol-binary128f))
    (db-field-type-binary256f (return scm-symbol-binary256f))
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

(define (scm-from-db-index-fields a) (SCM db-index-t*)
  "db-index-t* -> SCM:((field-offset . field-name) ...)"
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

(define (scm-from-field-data a field-type) (SCM db-record-value-t db-field-type-t)
  status-declare
  (declare b SCM)
  (if (not a.data) (return SCM-BOOL-F))
  (case = field-type
    ( (db-field-type-binary8
        db-field-type-binary16
        db-field-type-binary32
        db-field-type-binary64
        db-field-type-binary8f
        db-field-type-binary16f
        db-field-type-binary32f
        db-field-type-binary64f db-field-type-binary128f db-field-type-binary256f)
      (set b (scm-c-make-bytevector a.size))
      (memcpy (SCM_BYTEVECTOR_CONTENTS b) a.data a.size) (return b))
    ( (db-field-type-string8
        db-field-type-string16
        db-field-type-string32
        db-field-type-string64
        db-field-type-string8f
        db-field-type-string16f
        db-field-type-string32f
        db-field-type-string64f db-field-type-string128f db-field-type-string256f)
      (return (scm-from-utf8-stringn a.data a.size)))
    (db-field-type-uint64f (return (scm-from-uint64 (pointer-get (convert-type a.data uint64-t*)))))
    (db-field-type-uint32f (return (scm-from-uint32 (pointer-get (convert-type a.data uint32-t*)))))
    (db-field-type-uint16f (return (scm-from-uint16 (pointer-get (convert-type a.data uint16-t*)))))
    (db-field-type-uint8f (return (scm-from-uint8 (pointer-get (convert-type a.data uint8-t*)))))
    (db-field-type-int64f (return (scm-from-int64 (pointer-get (convert-type a.data int64-t*)))))
    (db-field-type-int32f (return (scm-from-int32 (pointer-get (convert-type a.data int32-t*)))))
    (db-field-type-int16f (return (scm-from-int16 (pointer-get (convert-type a.data int16-t*)))))
    (db-field-type-int8f (return (scm-from-int8 (pointer-get (convert-type a.data int8-t*)))))
    (db-field-type-float64f (return (scm-from-double (pointer-get (convert-type a.data double*)))))
    ( (db-field-type-uint128f db-field-type-uint256f db-field-type-int128f db-field-type-int256f)
      (set b (scm-c-make-bytevector a.size))
      (memcpy (SCM-BYTEVECTOR-CONTENTS b) a.data a.size)
      (return
        (scm-first (scm-bytevector->uint-list b scm-endianness-little (scm-from-size-t a.size)))))
    (else (status-set-both-goto status-group-db-guile status-id-field-value-invalid)))
  (label exit
    (scm-from-status-return SCM-UNSPECIFIED)))

(define (scm->field-data-integer scm-a field-type result-data result-size result-needs-free)
  (status-t SCM db-field-type-t void** size-t* boolean*)
  status-declare
  (declare
    b SCM
    size size-t
    data void*)
  (scm-dynwind-begin 0)
  (case = field-type
    (db-field-type-uint8f
      (set
        size 1
        *result-needs-free #t)
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0)
      (set (pointer-get (convert-type data uint16-t*)) (scm->uint8 scm-a)))
    (db-field-type-uint16f
      (set
        size 2
        *result-needs-free #t)
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0)
      (set (pointer-get (convert-type data uint16-t*)) (scm->uint16 scm-a)))
    (db-field-type-uint32f
      (set
        size 4
        *result-needs-free #t)
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0)
      (set (pointer-get (convert-type data uint32-t*)) (scm->uint32 scm-a)))
    (db-field-type-uint64f
      (set
        size 8
        *result-needs-free #t)
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0)
      (set (pointer-get (convert-type data uint64-t*)) (scm->uint64 scm-a)))
    (db-field-type-uint128f
      (set
        size 16
        *result-needs-free #t
        b (scm-uint-list->bytevector (scm-list-1 scm-a) scm-endianness-little (scm-from-size-t size)))
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0) (memcpy data (SCM-BYTEVECTOR-CONTENTS b) size))
    (db-field-type-uint256f
      (set
        size 32
        *result-needs-free #t
        b (scm-uint-list->bytevector (scm-list-1 scm-a) scm-endianness-little (scm-from-size-t size)))
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0) (memcpy data (SCM-BYTEVECTOR-CONTENTS b) size))
    (db-field-type-int8f
      (set
        size 1
        *result-needs-free #t)
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0)
      (set (pointer-get (convert-type data int8-t*)) (scm->int8 scm-a)))
    (db-field-type-int16f
      (set
        size 2
        *result-needs-free #t)
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0)
      (set (pointer-get (convert-type data int16-t*)) (scm->int16 scm-a)))
    (db-field-type-int32f
      (set
        size 4
        *result-needs-free #t)
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0)
      (set (pointer-get (convert-type data int32-t*)) (scm->int32 scm-a)))
    (db-field-type-int64f
      (set
        size 8
        *result-needs-free #t)
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0)
      (set (pointer-get (convert-type data int64-t*)) (scm->int64 scm-a)))
    (db-field-type-int128f
      (set
        size 16
        *result-needs-free #t
        b (scm-sint-list->bytevector (scm-list-1 scm-a) scm-endianness-little (scm-from-size-t size)))
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0) (memcpy data (SCM-BYTEVECTOR-CONTENTS b) size))
    (db-field-type-int256f
      (set
        size 32
        *result-needs-free #t
        b (scm-sint-list->bytevector (scm-list-1 scm-a) scm-endianness-little (scm-from-size-t size)))
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0) (memcpy data (SCM-BYTEVECTOR-CONTENTS b) size))
    (else (status-set-both-goto status-group-db-guile status-id-field-value-invalid)))
  (set
    *result-data data
    *result-size size)
  (label exit
    (scm-dynwind-end)
    (return status)))

(define (scm->field-data-string scm-a field-type result-data result-size result-needs-free)
  (status-t SCM db-field-type-t void** size-t* boolean*)
  status-declare
  (declare
    size size-t
    data void*)
  (set size (scm-c-string-utf8-length scm-a))
  (case = field-type
    ( (db-field-type-string8 db-field-type-string16 db-field-type-string32 db-field-type-string64)
      #t)
    (db-field-type-string8f
      (if (< 1 size) (status-set-both-goto status-group-db-guile status-id-field-value-invalid)))
    (db-field-type-string16f
      (if (< 2 size) (status-set-both-goto status-group-db-guile status-id-field-value-invalid)))
    (db-field-type-string32f
      (if (< 4 size) (status-set-both-goto status-group-db-guile status-id-field-value-invalid)))
    (db-field-type-string64f
      (if (< 8 size) (status-set-both-goto status-group-db-guile status-id-field-value-invalid)))
    (db-field-type-string128f
      (if (< 16 size) (status-set-both-goto status-group-db-guile status-id-field-value-invalid)))
    (db-field-type-string256f
      (if (< 32 size) (status-set-both-goto status-group-db-guile status-id-field-value-invalid)))
    (else (status-set-both-goto status-group-db-guile status-id-field-value-invalid)))
  (set
    *result-needs-free #f
    *result-data (scm->utf8-stringn scm-a 0)
    *result-size size)
  (label exit
    (return status)))

(define (scm->field-data-bytevector scm-a field-type result-data result-size result-needs-free)
  (status-t SCM db-field-type-t void** size-t* boolean*)
  status-declare
  (declare size size-t)
  (set size (SCM-BYTEVECTOR-LENGTH scm-a))
  (case = field-type
    ( (db-field-type-binary8 db-field-type-binary16 db-field-type-binary32 db-field-type-binary64)
      #t)
    (db-field-type-binary8f
      (if (< 1 size) (status-set-both-goto status-group-db-guile status-id-field-value-invalid)))
    (db-field-type-binary16f
      (if (< 2 size) (status-set-both-goto status-group-db-guile status-id-field-value-invalid)))
    (db-field-type-binary32f
      (if (< 4 size) (status-set-both-goto status-group-db-guile status-id-field-value-invalid)))
    (db-field-type-binary64f
      (if (< 8 size) (status-set-both-goto status-group-db-guile status-id-field-value-invalid)))
    (db-field-type-binary128f
      (if (< 16 size) (status-set-both-goto status-group-db-guile status-id-field-value-invalid)))
    (db-field-type-binary256f
      (if (< 32 size) (status-set-both-goto status-group-db-guile status-id-field-value-invalid)))
    (else (status-set-both-goto status-group-db-guile status-id-field-value-invalid)))
  (set
    *result-needs-free #f
    *result-data (SCM-BYTEVECTOR-CONTENTS scm-a)
    *result-size size)
  (label exit
    (return status)))

(define (scm->field-data-float scm-a field-type result-data result-size result-needs-free)
  (status-t SCM db-field-type-t void** size-t* boolean*)
  status-declare
  (declare
    size size-t
    data void*)
  (scm-dynwind-begin 0)
  (sc-comment "there is no scm->float")
  (case = field-type
    (db-field-type-float64f
      (set size 8)
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0)
      (set (pointer-get (convert-type data double*)) (scm->double scm-a)))
    (else (status-set-both-goto status-group-db-guile status-id-field-value-invalid)))
  (set
    *result-needs-free #t
    *result-data data
    *result-size size)
  (label exit
    (scm-dynwind-end)
    (return status)))

(define (scm->field-data scm-a field-type result-data result-size result-needs-free)
  (status-t SCM db-field-type-t void** size-t* boolean*)
  "convert an scm value to the format that will be used to for insert.
  result-data has to be freed by the caller only if result-needs-free is true.
  checks if the size of the data fits the field size"
  status-declare
  (cond
    ( (scm-is-bytevector scm-a)
      (return
        (scm->field-data-bytevector scm-a field-type result-data result-size result-needs-free)))
    ( (scm-is-string scm-a)
      (return (scm->field-data-string scm-a field-type result-data result-size result-needs-free)))
    ( (scm-is-integer scm-a)
      (return (scm->field-data-integer scm-a field-type result-data result-size result-needs-free)))
    ( (scm-is-rational scm-a)
      (return (scm->field-data-float scm-a field-type result-data result-size result-needs-free)))
    (else (status-set-both-goto status-group-db-guile status-id-field-value-invalid)))
  (label exit
    (return status)))

(define (scm->db-ids scm-a result) (status-t SCM db-ids-t*)
  "this routine allocates result and passes ownership to the caller"
  status-declare
  (db-ids-declare b)
  (declare length size-t)
  (set length (scm->size-t (scm-length scm-a)))
  (scm-dynwind-begin 0)
  (status-require (db-ids-new length &b))
  (scm-dynwind-unwind-handler free b.start 0)
  (while (not (scm-is-null scm-a))
    (db-ids-add b (scm->uintmax (scm-first scm-a)))
    (set scm-a (scm-tail scm-a)))
  (set *result b)
  (label exit
    (scm-dynwind-end)
    (return status)))

(define (scm-from-db-ids a) (SCM db-ids-t)
  (declare b SCM)
  (set b SCM-EOL)
  (while (db-ids-in-range a)
    (set b (scm-cons (scm-from-uint (db-ids-get a)) b))
    (db-ids-forward a))
  (return b))

(define (scm-from-db-records a) (SCM db-records-t)
  (declare
    b db-record-t*
    result SCM)
  (set result SCM-EOL)
  (while (i-array-in-range a)
    (set
      b (scm-gc-malloc (sizeof db-record-t) "db-record-t")
      (pointer-get b) (i-array-get a)
      result (scm-cons (scm-from-db-record b) result))
    (i-array-forward a))
  (return result))

(define (scm-from-db-relations a) (SCM db-relations-t)
  (declare
    b SCM
    record db-relation-t)
  (set b SCM-EOL)
  (while (db-relations-in-range a)
    (set
      record (db-relations-get a)
      b
      (scm-cons
        (scm-vector
          (scm-list-4
            (scm-from-uint record.left)
            (scm-from-uint record.right) (scm-from-uint record.label) (scm-from-uint record.ordinal)))
        b))
    (db-relations-forward a))
  (return b))

(define (db-guile-ordinal-generator state) (db-ordinal-t void*)
  (declare
    scm-state SCM
    scm-generator SCM
    scm-result SCM)
  (set
    scm-state (pointer-get (convert-type state SCM*))
    scm-generator (scm-first scm-state)
    scm-result (scm-apply-0 scm-generator (scm-tail scm-state))
    (pointer-get (convert-type state SCM*)) (scm-cons scm-generator (scm-tail scm-result)))
  (return (scm->uintmax (scm-first scm-result))))

(define (db-guile-record-matcher type record state) (boolean db-type-t* db-record-t void*)
  (declare
    scm-state SCM
    scm-matcher SCM
    scm-result SCM)
  (set
    scm-state (pointer-get (convert-type state SCM*))
    scm-matcher (scm-first scm-state)
    scm-result
    (scm-apply-2
      scm-matcher (scm-from-db-type type) (scm-from-db-record &record) (scm-tail scm-state))
    (pointer-get (convert-type state SCM*)) (scm-cons scm-matcher (scm-tail scm-result)))
  (return (scm->bool (scm-first scm-result))))

(pre-include "./selections.c")