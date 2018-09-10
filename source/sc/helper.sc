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
  (scm->db-selection a) (convert-type (scm-foreign-object-ref a 0) db-guile-selection-t*)
  ; error handling
  db-status-group-db-guile db-status-group-last
  (scm-from-status-error a) (scm-c-error (db-guile-status-name a) (db-guile-status-description a))
  (scm-c-error name description)
  (scm-call-1
    scm-rnrs-raise
    (scm-list-3
      (scm-from-latin1-symbol name)
      (scm-cons (scm-from-latin1-symbol "description") (scm-from-utf8-string description))
      (scm-cons (scm-from-latin1-symbol "c-routine") (scm-from-latin1-symbol __FUNCTION__))))
  (scm-from-status-return result) (return (scm-from-status result))
  (scm-from-status result)
  (if* status-is-success result
    (scm-from-status-error status)))

(declare
  scm-rnrs-raise SCM
  scm-symbol-binary SCM
  scm-symbol-binary8 SCM
  scm-symbol-binary16 SCM
  scm-symbol-binary32 SCM
  scm-symbol-binary64 SCM
  scm-symbol-float32 SCM
  scm-symbol-float64 SCM
  scm-symbol-int16 SCM
  scm-symbol-int32 SCM
  scm-symbol-int64 SCM
  scm-symbol-int8 SCM
  scm-symbol-min SCM
  scm-symbol-max SCM
  scm-symbol-label SCM
  scm-symbol-left SCM
  scm-symbol-ordinal SCM
  scm-symbol-right SCM
  scm-symbol-string SCM
  scm-symbol-string16 SCM
  scm-symbol-string32 SCM
  scm-symbol-string64 SCM
  scm-symbol-string8 SCM
  scm-symbol-uint16 SCM
  scm-symbol-uint32 SCM
  scm-symbol-uint64 SCM
  scm-symbol-uint8 SCM
  scm-symbol-uint128 SCM
  scm-symbol-uint256 SCM
  scm-symbol-uint512 SCM
  scm-symbol-int128 SCM
  scm-symbol-int256 SCM
  scm-symbol-int512 SCM
  scm-symbol-string128 SCM
  scm-symbol-string256 SCM
  scm-symbol-string512 SCM
  scm-symbol-binary128 SCM
  scm-symbol-binary256 SCM
  scm-symbol-binary512 SCM
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
    (scm-symbol-string (return db-field-type-string))
    (scm-symbol-binary (return db-field-type-binary))
    (scm-symbol-binary8 (return db-field-type-binary8))
    (scm-symbol-binary16 (return db-field-type-binary16))
    (scm-symbol-binary32 (return db-field-type-binary32))
    (scm-symbol-binary64 (return db-field-type-binary64))
    (scm-symbol-uint8 (return db-field-type-uint8))
    (scm-symbol-uint16 (return db-field-type-uint16))
    (scm-symbol-uint32 (return db-field-type-uint32))
    (scm-symbol-uint64 (return db-field-type-uint64))
    (scm-symbol-int8 (return db-field-type-int8))
    (scm-symbol-int16 (return db-field-type-int16))
    (scm-symbol-int32 (return db-field-type-int32))
    (scm-symbol-int64 (return db-field-type-int64))
    (scm-symbol-string8 (return db-field-type-string8))
    (scm-symbol-string16 (return db-field-type-string16))
    (scm-symbol-string32 (return db-field-type-string32))
    (scm-symbol-string64 (return db-field-type-string64))
    (scm-symbol-string128 (return db-field-type-string128))
    (scm-symbol-string256 (return db-field-type-string256))
    (scm-symbol-string512 (return db-field-type-string512))
    (scm-symbol-int128 (return db-field-type-int128))
    (scm-symbol-int256 (return db-field-type-int256))
    (scm-symbol-int512 (return db-field-type-int512))
    (scm-symbol-uint128 (return db-field-type-uint128))
    (scm-symbol-uint256 (return db-field-type-uint256))
    (scm-symbol-uint512 (return db-field-type-uint512))
    (scm-symbol-binary128 (return db-field-type-binary128))
    (scm-symbol-binary256 (return db-field-type-binary256))
    (scm-symbol-binary512 (return db-field-type-binary512))
    (else (return 0))))

(define (scm-from-db-field-type a) (SCM db-field-type-t)
  (case = a
    (db-field-type-string (return scm-symbol-string))
    (db-field-type-binary (return scm-symbol-binary))
    (db-field-type-binary8 (return scm-symbol-binary8))
    (db-field-type-binary16 (return scm-symbol-binary16))
    (db-field-type-binary32 (return scm-symbol-binary32))
    (db-field-type-binary64 (return scm-symbol-binary64))
    (db-field-type-uint8 (return scm-symbol-uint8))
    (db-field-type-uint16 (return scm-symbol-uint16))
    (db-field-type-uint32 (return scm-symbol-uint32))
    (db-field-type-uint64 (return scm-symbol-uint64))
    (db-field-type-int8 (return scm-symbol-int8))
    (db-field-type-int16 (return scm-symbol-int16))
    (db-field-type-int32 (return scm-symbol-int32))
    (db-field-type-int64 (return scm-symbol-int64))
    (db-field-type-string8 (return scm-symbol-string8))
    (db-field-type-string16 (return scm-symbol-string16))
    (db-field-type-string32 (return scm-symbol-string32))
    (db-field-type-string64 (return scm-symbol-string64))
    (db-field-type-float64 (return scm-symbol-float64))
    (db-field-type-string128 (return scm-symbol-string128))
    (db-field-type-string256 (return scm-symbol-string256))
    (db-field-type-string512 (return scm-symbol-string512))
    (db-field-type-int128 (return scm-symbol-int128))
    (db-field-type-int256 (return scm-symbol-int256))
    (db-field-type-int512 (return scm-symbol-int512))
    (db-field-type-uint128 (return scm-symbol-uint128))
    (db-field-type-uint256 (return scm-symbol-uint256))
    (db-field-type-uint512 (return scm-symbol-uint512))
    (db-field-type-binary128 (return scm-symbol-binary128))
    (db-field-type-binary256 (return scm-symbol-binary256))
    (db-field-type-binary512 (return scm-symbol-binary512))
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
  (case = field-type
    ( (db-field-type-binary
        db-field-type-binary64
        db-field-type-binary32
        db-field-type-binary16
        db-field-type-binary8 db-field-type-binary128 db-field-type-binary256 db-field-type-binary512)
      (set b (scm-c-make-bytevector a.size))
      (memcpy (SCM_BYTEVECTOR_CONTENTS b) a.data a.size) (return b))
    ( (db-field-type-string
        db-field-type-string64
        db-field-type-string32
        db-field-type-string16
        db-field-type-string8 db-field-type-string512 db-field-type-string256 db-field-type-string128)
      (return (scm-from-utf8-stringn a.data a.size)))
    (db-field-type-uint64 (return (scm-from-uint64 (pointer-get (convert-type a.data uint64-t*)))))
    (db-field-type-uint32 (return (scm-from-uint32 (pointer-get (convert-type a.data uint32-t*)))))
    (db-field-type-uint16 (return (scm-from-uint16 (pointer-get (convert-type a.data uint16-t*)))))
    (db-field-type-uint8 (return (scm-from-uint8 (pointer-get (convert-type a.data uint8-t*)))))
    (db-field-type-int64 (return (scm-from-int64 (pointer-get (convert-type a.data int64-t*)))))
    (db-field-type-int32 (return (scm-from-int32 (pointer-get (convert-type a.data int32-t*)))))
    (db-field-type-int16 (return (scm-from-int16 (pointer-get (convert-type a.data int16-t*)))))
    (db-field-type-int8 (return (scm-from-int8 (pointer-get (convert-type a.data int8-t*)))))
    (db-field-type-float64 (return (scm-from-double (pointer-get (convert-type a.data double*)))))
    ( (db-field-type-uint128
        db-field-type-uint256
        db-field-type-uint512 db-field-type-int128 db-field-type-int256 db-field-type-int512)
      (set b (scm-c-make-bytevector a.size))
      (memcpy (SCM-BYTEVECTOR-CONTENTS b) a.data a.size)
      (return
        (scm-first (scm-bytevector->uint-list b scm-endianness-little (scm-from-size-t a.size)))))
    (else (status-set-id-goto status-id-field-value-invalid)))
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
    (db-field-type-uint64
      (set
        size 8
        *result-needs-free #t)
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0)
      (set (pointer-get (convert-type data uint64-t*)) (scm->uint64 scm-a)))
    (db-field-type-uint32
      (set
        size 4
        *result-needs-free #t)
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0)
      (set (pointer-get (convert-type data uint32-t*)) (scm->uint32 scm-a)))
    (db-field-type-uint16
      (set
        size 2
        *result-needs-free #t)
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0)
      (set (pointer-get (convert-type data uint16-t*)) (scm->uint16 scm-a)))
    (db-field-type-uint8
      (set
        size 1
        *result-needs-free #t)
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0)
      (set (pointer-get (convert-type data uint16-t*)) (scm->uint8 scm-a)))
    (db-field-type-int64
      (set
        size 8
        *result-needs-free #t)
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0)
      (set (pointer-get (convert-type data int64-t*)) (scm->int64 scm-a)))
    (db-field-type-int32
      (set
        size 4
        *result-needs-free #t)
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0)
      (set (pointer-get (convert-type data int32-t*)) (scm->int32 scm-a)))
    (db-field-type-int16
      (set
        size 2
        *result-needs-free #t)
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0)
      (set (pointer-get (convert-type data int16-t*)) (scm->int16 scm-a)))
    (db-field-type-int8
      (set
        size 1
        *result-needs-free #t)
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0)
      (set (pointer-get (convert-type data int8-t*)) (scm->int8 scm-a)))
    (db-field-type-uint512
      (set
        size 64
        *result-needs-free #t
        b (scm-uint-list->bytevector (scm-list-1 scm-a) scm-endianness-little (scm-from-size-t size)))
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0) (memcpy data (SCM-BYTEVECTOR-CONTENTS b) size))
    (db-field-type-uint256
      (set
        size 32
        *result-needs-free #t
        b (scm-uint-list->bytevector (scm-list-1 scm-a) scm-endianness-little (scm-from-size-t size)))
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0) (memcpy data (SCM-BYTEVECTOR-CONTENTS b) size))
    (db-field-type-uint128
      (set
        size 16
        *result-needs-free #t
        b (scm-uint-list->bytevector (scm-list-1 scm-a) scm-endianness-little (scm-from-size-t size)))
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0) (memcpy data (SCM-BYTEVECTOR-CONTENTS b) size))
    (db-field-type-int512
      (set
        size 64
        *result-needs-free #t
        b (scm-sint-list->bytevector (scm-list-1 scm-a) scm-endianness-little (scm-from-size-t size)))
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0) (memcpy data (SCM-BYTEVECTOR-CONTENTS b) size))
    (db-field-type-int256
      (set
        size 32
        *result-needs-free #t
        b (scm-sint-list->bytevector (scm-list-1 scm-a) scm-endianness-little (scm-from-size-t size)))
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0) (memcpy data (SCM-BYTEVECTOR-CONTENTS b) size))
    (db-field-type-int128
      (set
        size 16
        *result-needs-free #t
        b (scm-sint-list->bytevector (scm-list-1 scm-a) scm-endianness-little (scm-from-size-t size)))
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0) (memcpy data (SCM-BYTEVECTOR-CONTENTS b) size))
    (else (status-set-id-goto status-id-field-value-invalid)))
  (set
    *result-data data
    *result-size size)
  (label exit
    (scm-dynwind-end)
    (return status)))

(define (scm->field-data-bytevector scm-a field-type result-data result-size result-needs-free)
  (status-t SCM db-field-type-t void** size-t* boolean*)
  status-declare
  (declare
    size size-t
    data void*)
  (case = field-type
    (db-field-type-binary #t)
    (db-field-type-binary512 (if (< 64 size) (status-set-id-goto status-id-field-value-invalid)))
    (db-field-type-binary256 (if (< 32 size) (status-set-id-goto status-id-field-value-invalid)))
    (db-field-type-binary128 (if (< 16 size) (status-set-id-goto status-id-field-value-invalid)))
    (db-field-type-binary64 (if (< 8 size) (status-set-id-goto status-id-field-value-invalid)))
    (db-field-type-binary32 (if (< 4 size) (status-set-id-goto status-id-field-value-invalid)))
    (db-field-type-binary16 (if (< 2 size) (status-set-id-goto status-id-field-value-invalid)))
    (db-field-type-binary8 (if (< 1 size) (status-set-id-goto status-id-field-value-invalid)))
    (else (status-set-id-goto status-id-field-value-invalid)))
  (set
    *result-needs-free #f
    *result-data (SCM-BYTEVECTOR-CONTENTS scm-a)
    *result-size (SCM-BYTEVECTOR-LENGTH scm-a))
  (label exit
    (return status)))

(define (scm->field-data-string scm-a field-type result-data result-size result-needs-free)
  (status-t SCM db-field-type-t void** size-t* boolean*)
  status-declare
  (declare
    size size-t
    data void*)
  (set size (scm-c-string-utf8-length scm-a))
  (case = field-type
    (db-field-type-string #t)
    (db-field-type-string512 (if (< 64 size) (status-set-id-goto status-id-field-value-invalid)))
    (db-field-type-string256 (if (< 32 size) (status-set-id-goto status-id-field-value-invalid)))
    (db-field-type-string128 (if (< 16 size) (status-set-id-goto status-id-field-value-invalid)))
    (db-field-type-string64 (if (< 8 size) (status-set-id-goto status-id-field-value-invalid)))
    (db-field-type-string32 (if (< 4 size) (status-set-id-goto status-id-field-value-invalid)))
    (db-field-type-string16 (if (< 2 size) (status-set-id-goto status-id-field-value-invalid)))
    (db-field-type-string8 (if (< 1 size) (status-set-id-goto status-id-field-value-invalid)))
    (else (status-set-id-goto status-id-field-value-invalid)))
  (set
    *result-needs-free #f
    *result-data (scm->utf8-stringn scm-a 0)
    *result-size size)
  (label exit
    (return status)))

(define (scm->field-data-float scm-a field-type result-data result-size result-needs-free)
  (status-t SCM db-field-type-t void** size-t* boolean*)
  status-declare
  (declare
    size size-t
    data void*)
  (case = field-type
    (db-field-type-float64
      (set size 8)
      (status-require (db-helper-malloc size &data))
      (scm-dynwind-unwind-handler free data 0)
      (set (pointer-get (convert-type data double*)) (scm->double scm-a)))
    (else
      (sc-comment "for some reason there is no scm->float")
      (status-set-id-goto status-id-field-value-invalid)))
  (set
    *result-needs-free #t
    *result-data data
    *result-size size)
  (label exit
    (return status)))

(define (scm->field-data scm-a field-type result-data result-size result-needs-free)
  (status-t SCM db-field-type-t void** size-t* boolean*)
  "convert an scm value to the format that will be used to for insert.
  result-data has to be freed by the caller only if result-needs-free is true.
  checks if the size of the data fits the field size"
  status-declare
  (declare
    size size-t
    data void*)
  (cond
    ( (scm-is-bytevector scm-a)
      (scm->field-data-bytevector scm-a field-type result-data result-size result-needs-free))
    ( (scm-is-string scm-a)
      (scm->field-data-string scm-a field-type result-data result-size result-needs-free))
    ( (scm-is-integer scm-a)
      (scm->field-data-integer scm-a field-type result-data result-size result-needs-free))
    ( (scm-is-rational scm-a)
      (scm->field-data-float scm-a field-type result-data result-size result-needs-free))
    (else (status-set-id-goto status-id-field-value-invalid)))
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
    (db-ids-add b (scm->uint (scm-first scm-a)))
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
    (pointer-get (convert-type state SCM*)) (scm-cons scm-generator scm-result))
  (return (scm->uint (scm-first scm-result))))

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
    (pointer-get (convert-type state SCM*)) (scm-cons scm-matcher scm-result))
  (return (scm->bool (scm-first scm-result))))

(pre-include "./selections.c")