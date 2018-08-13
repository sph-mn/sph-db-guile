(sc-comment
  "configuration options"
  "possible types correspond to guiles integer conversion routines. for example uint, int32, uint64, intmax")

(pre-define-if-not-defined db-data-integer-type int64)
(pre-define scm-enable-typechecks? #t)
(pre-include "sph-db.h" "libguile.h" "sph/one" "sph/guile")
(pre-define (status->scm-return result) (return (status->scm result)))

(pre-define (optional-count a)
  (if* (scm-is-integer a) (scm->uint a)
    0))

(pre-define (optional-count-one a)
  (if* (scm-is-integer a) (scm->uint a)
    1))

(pre-define (optional-every? a) (or (scm-is-undefined a) (scm-is-true a)))

(pre-define (optional-types a)
  (if* (scm-is-integer a) (scm->uint8 a)
    0))

(pre-define (optional-offset a)
  (if* (scm-is-integer a) (scm->uint32 a)
    0))

(pre-define (scm->txn a) (convert-type (SCM-SMOB-DATA a) db-txn-t*))
(pre-define (db-id->scm a) (scm-from-uint a))
(pre-define (scm->db-id a) (scm->uint a))
(pre-define db-guile-scm->ordinal scm->int)

(pre-define
  db-guile-type-bytevector 0
  db-guile-type-integer 1
  db-guile-type-string 2
  db-guile-type-rational 3
  db-guile-type-scheme 4)


  db-field-type-float32 4
  db-field-type-float64 6
  db-field-type-binary 1
  db-field-type-string 3
  db-field-type-int8 48
  db-field-type-int16 80
  db-field-type-int32 112
  db-field-type-int64 144
  db-field-type-uint8 32
  db-field-type-uint16 64
  db-field-type-uint32 96
  db-field-type-uint64 128
  db-field-type-char8 34
  db-field-type-char16 66
  db-field-type-char32 98
  db-field-type-char64 130

#;(
(pre-define
  (optional-ids scm-a a)
  "append ids from scm-a to db-ids-t* list if a is not false or undefined (no filter)"
  (if (scm-is-pair scm-a) (status-require! (scm->db-ids scm-a (address-of a)))))

(pre-define (optional-relation-retrieve a)
  (if* (scm-is-symbol a)
    (case* scm-is-eq a
      (scm-symbol-right db-relation-records->scm-retrieve-right)
      (scm-symbol-left db-relation-records->scm-retrieve-left)
      (scm-symbol-label db-relation-records->scm-retrieve-label)
      (scm-symbol-ordinal db-relation-records->scm-retrieve-ordinal)
      (else 0))
    db-relation-records->scm))

(pre-define (txn->scm txn-pointer)
  (scm-new-smob scm-type-txn (convert-type txn-pointer scm-t-bits)))

(define-type db-read-state-type-t (enum (db-read-state-type-relation db-read-state-type-node)))

(define-type
  db-guile-relation-read-state-t
  (struct
    (left db-ids-t*)
    (right db-ids-t*)
    (label db-ids-t*)
    (records->scm (function-pointer SCM db-relation-records-t*))
    (db-state db-relation-read-state-t)))

(define-type
  db-guile-generic-read-state-t
  (struct
    (state b0*)
    (read-state-type db-read-state-type-t)))

(pre-define
  mi-list-name-prefix db-guile-generic-read-states
  mi-list-element-t db-guile-generic-read-state-t)

(sc-include-once
  sph-mi-list "sph/mi-list")

(pre-define
  db-guile-generic-read-states-first mi-list-first
  db-guile-generic-read-states-rest mi-list-rest)

(define active-read-states (__thread db-guile-generic-read-states-t*) 0)

(pre-define
  (active-read-states-add! state read-state-type)
  "create a generic-read-state object with the given state and read-state-type and
  add it to the thread-local active-read-states list"
  (define generic-read-state db-guile-generic-read-state-t (struct-literal state read-state-type))
  (define generic-read-states-temp db-guile-generic-read-states-t*
    (db-guile-generic-read-states-add active-read-states generic-read-state))
  (if generic-read-states-temp (set active-read-states generic-read-states-temp)
    (status-set-both db-status-group-dg db-status-id-memory)))

(define (active-read-states-free) b0
  "read states are freed when the transaction is finalised. there can only be one transaction per thread.
  dg read states may be wrapped in db-guile read states to carry pointers to values that are to be
  garbage collected"
  (define generic-read-state db-guile-generic-read-state-t)
  (while active-read-states
    (set generic-read-state (db-guile-generic-read-states-first active-read-states))
    (case = (struct-get generic-read-state read-state-type)
      (db-read-state-type-relation
        (define state db-guile-relation-read-state-t* (struct-get generic-read-state state))
        (db-relation-selection-destroy (address-of (struct-pointer-get state db-state)))
        (db-ids-destroy (struct-pointer-get state left))
        (db-ids-destroy (struct-pointer-get state label))
        (db-ids-destroy (struct-pointer-get state right)) (free state))
      (db-read-state-type-node
        (define state db-node-read-state-t* (struct-get generic-read-state state))
        (db-node-selection-destroy state)))
    (set active-read-states (db-guile-generic-read-states-drop active-read-states))))

(define
  scm-type-selection scm-t-bits
  scm-type-txn scm-t-bits
  db-scm-write SCM
  db-scm-read SCM
  scm-rnrs-raise SCM
  scm-symbol-label SCM
  scm-symbol-left SCM
  scm-symbol-right SCM
  scm-symbol-ordinal SCM)

(define (selection->scm pointer) (SCM b0*)
  "with gcc optimisation level 3, not using a local variable did not set the smob data.
  passing a null pointer creates an empty/null selection"
  (define result SCM (scm-new-smob scm-type-selection (convert-type pointer scm-t-bits)))
  (return result))

(pre-define (scm->selection a type-group-name)
  (convert-type (SCM-SMOB-DATA a) (pre-concat dg_ type-group-name _read-state-t*)))

(pre-define (scm-c-error name description)
  (scm-call-1
    scm-rnrs-raise
    (scm-list-3
      (scm-from-latin1-symbol name)
      (scm-cons (scm-from-latin1-symbol "description") (scm-from-utf8-string description))
      (scm-cons (scm-from-latin1-symbol "c-routine") (scm-from-latin1-symbol __FUNCTION__)))))

(pre-define (status->scm-error a) (scm-c-error (db-status-name a) (db-status-description a)))

(pre-define (status->scm result)
  (if* status-success? result
    (status->scm-error status)))

(pre-define (define-db-type? name)
  (define ((pre-concat scm-dg_ name _p) id) (SCM SCM)
    (return (scm-from-bool (and (scm-is-integer id) ((pre-concat dg_ name _p) (scm->uint id)))))))

(pre-define (scm-string-octet-length-uint a)
  (scm->uint (scm-product (scm-string-bytes-per-char a) (scm-string-length a))))

(pre-define (db-pre-concat-primitive a b) (pre-concat a b))
(pre-define (db-pre-concat a b) (db-pre-concat-primitive a b))

(pre-define
  db-scm->data-integer (db-pre-concat scm_to_ db-data-integer-type)
  db-data-integer->scm (db-pre-concat scm_from_ db-data-integer-type)
  db-data-integer-t (db-pre-concat db-data-integer-type _t))

(pre-define (scm-c-alist-add-from-struct target source key struct-key convert)
  (set target
    (scm-acons (scm-from-latin1-symbol key) (convert (struct-get source struct-key)) target)))

(define scm-bytevector-null SCM)

(define (scm-string->db-data a result intern-type) (status-t SCM db-data-t* b8)
  "strings are stored without a trailing 0 because we have the octet size exactly"
  status-init
  (define a-size size-t)
  ; If lenp is not NULL, the string is not null terminated, and the length of the returned string is returned in lenp
  (define a-c b8* (scm->utf8-stringn a (address-of a-size)))
  (define size size-t (+ db-guile-intern-type-size a-size))
  (define data b8* (calloc size 1))
  (if (not data) (db-status-set-id-goto db-status-id-memory))
  ; the only guile binding that allows writing to a buffer is scm_to_locale_stringbuf,
  ; and it is current locale dependent. it also uses memcpy similarly internally
  (memcpy (+ db-guile-intern-type-size data) a-c (- size db-guile-intern-type-size))
  (pre-if db-guile-intern-type-size (set (deref data) intern-type))
  (struct-pointer-set result
    data data
    size size)
  (label exit
    (return status)))

(define (scm->db-data a result) (status-t SCM db-data-t*)
  "the caller has to free the data field in the result struct"
  status-init
  (cond
    ( (scm-is-bytevector a)
      (define size size-t (+ db-guile-intern-type-size (SCM-BYTEVECTOR-LENGTH a)))
      (define data b8* (calloc size 1))
      (if (not data) (db-status-set-id-goto db-status-id-memory))
      (pre-if db-guile-intern-type-size (set (deref data) db-guile-intern-bytevector))
      (memcpy (+ db-guile-intern-type-size data) (SCM-BYTEVECTOR-CONTENTS a) size)
      (struct-pointer-set result
        data data
        size size))
    ((scm-is-string a) (scm-string->db-data a result db-guile-intern-string))
    ( (scm-is-integer a)
      (define size size-t (+ db-guile-intern-type-size (sizeof db-data-integer-t)))
      (define data b8* (calloc size 1))
      (if (not data) (db-status-set-id-goto db-status-id-memory))
      (pre-if db-guile-intern-integer (set (deref data) db-guile-intern-integer))
      (set (deref (convert-type (+ db-guile-intern-type-size data) db-data-integer-t*))
        (db-scm->data-integer a))
      (struct-pointer-set result
        data data
        size size))
    ( (scm-is-rational a)
      (define size size-t (+ db-guile-intern-type-size (sizeof double)))
      (define data b8* (calloc size 1))
      (if (not data) (db-status-set-id-goto db-status-id-memory))
      (pre-if db-guile-intern-type-size (set (deref data) db-guile-intern-rational))
      (set (deref (convert-type (+ db-guile-intern-type-size data) double*)) (scm->double a)))
    (else
      (define b SCM (scm-object->string a db-scm-write))
      (scm-string->db-data b result db-guile-intern-scheme)))
  (label exit
    (return status)))

(define (db-data-list-data-free a) (b0 db-data-list-t*)
  (while a
    (free (struct-get (db-data-list-first a) data))
    (set a (db-data-list-rest a))))

(define (debug-display-data a size) (b0 b8* size-t)
  (if (not size) (return))
  (printf "%x" (deref a 0))
  (define index size-t 1)
  (while (< index size)
    (printf " %x" (deref a index))
    (set index (+ 1 index)))
  (printf "\n"))

(pre-define
  (define-db-data->scm type-group-name)
  (define ((pre-concat dg_ type-group-name _to-scm-bytevector) a)
    (SCM (pre-concat dg_ type-group-name _t))
    (define r SCM (scm-c-make-bytevector (- (struct-get a size) db-guile-intern-type-size)))
    (memcpy
      (SCM-BYTEVECTOR-CONTENTS r)
      (+ db-guile-intern-type-size (convert-type (struct-get a data) b8*))
      (- (struct-get a size) db-guile-intern-type-size))
    (return r))
  (define ((pre-concat dg_ type-group-name _to-scm-string) a)
    (SCM (pre-concat dg_ type-group-name _t))
    (scm-from-utf8-stringn
      (+ db-guile-intern-type-size (convert-type (struct-get a data) b8*))
      (- (struct-get a size) db-guile-intern-type-size)))
  (define ((pre-concat dg_ type-group-name _to-scm-integer) a)
    (SCM (pre-concat dg_ type-group-name _t))
    (if (> (struct-get a size) db-guile-intern-type-size)
      (db-data-integer->scm
        (deref
          (convert-type
            (+ db-guile-intern-type-size (convert-type (struct-get a data) b8*)) db-data-integer-t*)))
      (scm-from-int8 0)))
  (define ((pre-concat dg_ type-group-name _to-scm-rational) a)
    (SCM (pre-concat dg_ type-group-name _t))
    (if (> (struct-get a size) db-guile-intern-type-size)
      (scm-from-double
        (deref
          (convert-type
            (+ db-guile-intern-type-size (convert-type (struct-get a data) b8*)) double*)))
      (scm-from-int8 0)))
  (define ((pre-concat dg_ type-group-name _to-scm-scheme) a)
    (SCM (pre-concat dg_ type-group-name _t))
    (scm-call-with-input-string
      (scm-from-utf8-stringn
        (+ db-guile-intern-type-size (convert-type (struct-get a data) b8*))
        (- (struct-get a size) db-guile-intern-type-size))
      db-scm-read))
  (define ((pre-concat dg_ type-group-name _to_scm) a) (SCM (pre-concat dg_ type-group-name _t))
    (define type b8
      (if* db-guile-intern-type-size (deref (convert-type (struct-get a data) b8*))
        db-guile-intern-bytevector))
    ( (case* = type
        (db-guile-intern-bytevector (pre-concat dg_ type-group-name _to-scm-bytevector))
        (db-guile-intern-integer (pre-concat dg_ type-group-name _to-scm-integer))
        (db-guile-intern-string (pre-concat dg_ type-group-name _to-scm-string))
        (db-guile-intern-rational (pre-concat dg_ type-group-name _to-scm-rational))
        (db-guile-intern-scheme (pre-concat dg_ type-group-name _to-scm-scheme)))
      a)))

;db-data->scm-*
(define-db-data->scm data)
;db-data-record->scm-*
(define-db-data->scm data-record)

(define (db-ids->scm a) (SCM db-ids-t*)
  (define result SCM SCM-EOL)
  (while a
    (set
      result (scm-cons (db-id->scm (db-ids-first a)) result)
      a (db-ids-rest a)))
  (return result))

(define (scm->db-ids a result) (status-t SCM db-ids-t**)
  status-init
  (define result-temp db-ids-t* (deref result))
  (while (not (scm-is-null a))
    (set result-temp (db-ids-add result-temp (scm->db-id (scm-first a))))
    (if result-temp
      (set
        (deref result) result-temp
        a (scm-tail a))
      (begin
        (db-ids-destroy (deref result))
        (db-status-set-id-goto db-status-id-memory))))
  (label exit
    (return status)))

(define (db-data-list->scm a) (SCM db-data-list-t*)
  (define result SCM SCM-EOL)
  (while a
    (set
      result (scm-cons (db-data->scm (db-data-list-first a)) result)
      a (db-data-list-rest a)))
  (return result))

(define (db-data-records->scm a convert-data)
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

(define (db-relation-records->scm a) (SCM db-relation-records-t*)
  (define result SCM SCM-EOL)
  (define record db-relation-record-t)
  (while a
    (set
      record (db-relation-records-first a)
      result
      (scm-cons
        (scm-vector
          (scm-list-4
            (db-id->scm (struct-get record left))
            (db-id->scm (struct-get record right))
            (db-id->scm (struct-get record label)) (db-id->scm (struct-get record ordinal))))
        result)
      a (db-relation-records-rest a)))
  (return result))

(pre-define (define-db-relation-records->scm-retrieve field-name)
  (define ((pre-concat db-relation-records->scm-retrieve_ field-name) a)
    (SCM db-relation-records-t*)
    (define result SCM SCM-EOL)
    (define record db-relation-record-t)
    (while a
      (set
        record (db-relation-records-first a)
        result (scm-cons (db-id->scm (struct-get record field-name)) result)
        a (db-relation-records-rest a)))
    (return result)))

(define-db-relation-records->scm-retrieve left)
(define-db-relation-records->scm-retrieve right)
(define-db-relation-records->scm-retrieve label)
(define-db-relation-records->scm-retrieve ordinal)

(define (scm->db-data-list a result) (status-t SCM db-data-list-t**)
  status-init
  (define result-temp db-data-list-t* (deref result))
  (define data-temp db-data-t (struct-literal 0 0))
  (while (not (scm-is-null a))
    (status-require! (scm->db-data (scm-first a) (address-of data-temp)))
    (set result-temp (db-data-list-add result-temp data-temp))
    (if result-temp
      (set
        (deref result) result-temp
        a (scm-tail a))
      (db-status-set-id-goto db-status-id-memory)))
  (label exit
    ;added result elements are not freed on error to allow passing a result-value with elements
    (if status-failure? (free (struct-get data-temp data)))
    (return status)))

(define (scm-from-mdb-stat a) (SCM MDB-stat*)
  status-init
  (define result SCM SCM-EOL)
  (pre-let
    ( (result-add key struct-key)
      (scm-c-alist-add-from-struct result (deref a) key struct-key scm-from-uint))
    (result-add "ms-psize" ms-psize)
    (result-add "ms-depth" ms-depth)
    (result-add "ms-branch-pages" ms-branch-pages)
    (result-add "ms-leaf-pages" ms-leaf-pages)
    (result-add "ms-overflow-pages" ms-overflow-pages) (result-add "ms-entries" ms-entries))
  (status->scm-return result))

(define (scm-db-init scm-path scm-options) (SCM SCM SCM)
  status-init
  (define
    options db-init-options-t
    options-pointer db-init-options-t*
    scm-temp SCM)
  (define path b8* 0)
  (if (or (scm-is-undefined scm-options) (scm-is-null scm-options)) (set options-pointer 0)
    (pre-let
      ( (scm-get-value name)
        (begin
          (set scm-temp (scm-assoc-ref scm-options (scm-from-latin1-symbol name)))
          (set scm-temp
            (if* (scm-is-pair scm-temp) (scm-tail scm-temp)
              SCM-UNDEFINED))))
      (db-init-options-set-defaults (address-of options))
      (scm-get-value "read-only?")
      (if (scm-is-bool scm-temp) (set options.read-only? (scm-is-true scm-temp)))
      (scm-get-value "maximum-size-octets")
      (if (scm-is-integer scm-temp) (set options.maximum-size-octets (scm->uint scm-temp)))
      (scm-get-value "maximum-reader-count")
      (if (scm-is-integer scm-temp) (set options.maximum-reader-count (scm->uint scm-temp)))
      (scm-get-value "filesystem-has-ordered-writes?")
      (if (scm-is-bool scm-temp)
        (set options.filesystem-has-ordered-writes? (scm-is-true scm-temp)))
      (scm-get-value "env-open-flags")
      (if (scm-is-integer scm-temp) (set options.env-open-flags (scm->uint scm-temp)))
      (scm-get-value "file-permissions")
      (if (scm-is-integer scm-temp) (set options.file-permissions (scm->uint scm-temp)))
      (set options-pointer (address-of options))))
  (set path (scm->locale-string scm-path))
  (status-require! (db-init path options-pointer))
  (define scm-module SCM (scm-c-resolve-module "sph storage dg"))
  (set scm-temp (scm-variable-ref (scm-c-module-lookup scm-module "db-init-extension")))
  (while (not (scm-is-null scm-temp))
    (scm-call-0 (scm-first scm-temp))
    (set scm-temp (scm-tail scm-temp)))
  (label exit
    (free path)
    (status->scm-return SCM-BOOL-T)))

(define (scm-db-exit) SCM
  (scm-gc)
  (db-exit)
  (return SCM-UNSPECIFIED))

(define (scm-db-initialised?) SCM (return (scm-from-bool db-initialised)))
(define (scm-db-root) SCM (return (scm-from-locale-string db-root)))

(pre-define (define-scm-db-txn-create name flags)
  (define ((pre-concat scm-db-txn-create_ name)) SCM
    status-init
    (define txn db-txn-t*)
    (db-mdb-status-require! (mdb-txn-begin db-mdb-env 0 flags (address-of txn)))
    (define result SCM (txn->scm txn))
    (label exit
      (if (and result status-failure?) (free txn))
      (status->scm-return result))))

(define-scm-db-txn-create read MDB-RDONLY)
(define-scm-db-txn-create write 0)
(define-db-type? id)
(define-db-type? intern)
(define-db-type? extern)
(define-db-type? relation)

(define (scm-db-txn-abort scm-txn) (SCM SCM)
  (active-read-states-free)
  (mdb-txn-abort (scm->txn scm-txn))
  (SCM-SET-SMOB-DATA scm-txn 0)
  (return SCM-UNSPECIFIED))

(define (scm-db-txn-commit scm-txn) (SCM SCM)
  "note that mdb-txn-commit frees cursors - active-read-states-free uses mdb-cursor-close.
  if active-read-states-free is called after mdb-txn-commit a double free occurs"
  status-init
  (active-read-states-free)
  (db-mdb-status-require! (mdb-txn-commit (scm->txn scm-txn)))
  (SCM-SET-SMOB-DATA scm-txn 0)
  (label exit
    (status->scm-return SCM-UNSPECIFIED)))

(define (scm-db-id-create scm-txn scm-count) (SCM SCM SCM)
  status-init
  (define count b32 (optional-count-one scm-count))
  (define ids db-ids-t* 0)
  (status-require! (db-id-create (scm->txn scm-txn) count (address-of ids)))
  (define result SCM (db-ids->scm ids))
  (label exit
    (db-ids-destroy ids)
    (status->scm-return result)))

(define (scm-db-extern-create scm-txn scm-count scm-data) (SCM SCM SCM SCM)
  status-init
  (define count b32 (optional-count-one scm-count))
  (define data-struct db-data-t (struct-literal 0 0))
  (define data db-data-t* (address-of data-struct))
  (if (scm-is-undefined scm-data) (set data 0)
    (status-require! (scm->db-data scm-data data)))
  (define ids db-ids-t* 0)
  (status-require! (db-extern-create (scm->txn scm-txn) count data (address-of ids)))
  (define result SCM (db-ids->scm ids))
  (label exit
    (db-ids-destroy ids)
    (free (struct-get data-struct data))
    (status->scm-return result)))

(define (scm-db-extern-id->data scm-txn scm-ids scm-every?) (SCM SCM SCM SCM)
  status-init
  (define every? boolean (optional-every? scm-every?))
  (define ids db-ids-t* 0)
  (status-require! (scm->db-ids scm-ids (address-of ids)))
  (define data db-data-list-t* 0)
  (db-status-require-read! (db-extern-id->data (scm->txn scm-txn) ids every? (address-of data)))
  (define result SCM (db-data-list->scm data))
  (label exit
    (db-ids-destroy ids)
    (db-data-list-destroy data)
    (status->scm-return result)))

(define (scm-db-extern-data->id scm-txn scm-data) (SCM SCM SCM)
  status-init
  (define data db-data-t (struct-literal 0 0))
  (status-require! (scm->db-data scm-data (address-of data)))
  (define ids db-ids-t* 0)
  (db-status-require-read! (db-extern-data->id (scm->txn scm-txn) data (address-of ids)))
  (define result SCM (db-ids->scm ids))
  (label exit
    (db-ids-destroy ids)
    (free (struct-get data data))
    (status->scm-return result)))

(define (db-guile-ordinal-generator state) (db-ordinal-t b0*)
  (define scm-state SCM (deref (convert-type state SCM*)))
  (define scm-generator SCM (scm-first scm-state))
  (define scm-result SCM (scm-apply-0 scm-generator (scm-tail scm-state)))
  (set (deref (convert-type state SCM*)) (scm-cons scm-generator scm-result))
  (return (db-guile-scm->ordinal (scm-first scm-result))))

(define
  (scm-db-relation-ensure
    scm-txn scm-left scm-right scm-label scm-ordinal-generator scm-ordinal-generator-state)
  (SCM SCM SCM SCM SCM SCM SCM)
  status-init
  (if (or (scm-is-undefined scm-label) (not (scm-is-true scm-label)))
    (set scm-label (scm-list-1 (scm-from-uint8 0))))
  (db-define-ids-3 left right label)
  (status-require! (scm->db-ids scm-left (address-of left)))
  (status-require! (scm->db-ids scm-right (address-of right)))
  (if (scm-is-true scm-label) (status-require! (scm->db-ids scm-label (address-of label))))
  (define ordinal-generator db-relation-ordinal-generator-t 0)
  (define ordinal-generator-state b0*)
  (define ordinal-value db-ordinal-t)
  (define scm-state SCM)
  (if (scm-is-true (scm-procedure? scm-ordinal-generator))
    (set
      scm-state
      (scm-cons
        scm-ordinal-generator
        (if* (scm-is-true (scm-list? scm-ordinal-generator-state)) scm-ordinal-generator-state
          (scm-list-1 scm-ordinal-generator-state)))
      ordinal-generator-state (address-of scm-state)
      ordinal-generator db-guile-ordinal-generator)
    (set
      ordinal-value
      (if* (scm-is-undefined scm-ordinal-generator-state) 0
        (scm->uint scm-ordinal-generator-state))
      ordinal-generator-state (address-of ordinal-value)))
  (status-require!
    (db-relation-ensure
      (scm->txn scm-txn) left right label ordinal-generator ordinal-generator-state))
  (label exit
    (db-ids-destroy left)
    (db-ids-destroy right)
    (db-ids-destroy label)
    (status->scm-return SCM-BOOL-T)))

(define (scm-db-statistics scm-txn) (SCM SCM)
  status-init
  (define result SCM SCM-EOL)
  (define stat db-statistics-t)
  (status-require! (db-statistics (scm->txn scm-txn) (address-of stat)))
  (pre-let
    ( (result-add key struct-key)
      (set result
        (scm-acons
          (scm-from-latin1-symbol key)
          (scm-from-mdb-stat (address-of (struct-get stat struct-key))) result)))
    (result-add "id->data" id->data)
    (result-add "data-intern->id" data-intern->id)
    (result-add "data-extern->extern" data-extern->extern)
    (result-add "left->right" left->right)
    (result-add "right->left" right->left) (result-add "label->left" label->left))
  (label exit
    (status->scm-return result)))

(define (scm-db-delete scm-txn scm-ids) (SCM SCM SCM)
  status-init
  (define ids db-ids-t* 0)
  (status-require! (scm->db-ids scm-ids (address-of ids)))
  (status-require! (db-delete (scm->txn scm-txn) ids))
  (label exit
    (db-ids-destroy ids)
    (status->scm-return SCM-UNSPECIFIED)))

(define (scm-db-identify scm-txn scm-ids) (SCM SCM SCM)
  status-init
  (define ids db-ids-t* 0)
  (status-require! (scm->db-ids scm-ids (address-of ids)))
  (define ids-result db-ids-t* 0)
  (status-require! (db-identify (scm->txn scm-txn) ids (address-of ids-result)))
  (define result SCM (db-ids->scm ids-result))
  (label exit
    (db-ids-destroy ids)
    (db-ids-destroy ids-result)
    (status->scm-return result)))

(define (scm-db-exists? scm-txn scm-ids) (SCM SCM SCM)
  status-init
  (define ids db-ids-t* 0)
  (status-require! (scm->db-ids scm-ids (address-of ids)))
  (define result-c boolean)
  (status-require! (db-exists? (scm->txn scm-txn) ids (address-of result-c)))
  (label exit
    (db-ids-destroy ids)
    (status->scm-return (scm-from-bool result-c))))

(define (scm-db-intern-ensure scm-txn scm-data) (SCM SCM SCM)
  status-init
  (define data db-data-list-t* 0)
  (status-require! (scm->db-data-list scm-data (address-of data)))
  (define ids db-ids-t* 0)
  (status-require! (db-intern-ensure (scm->txn scm-txn) data (address-of ids)))
  (define result SCM (db-ids->scm ids))
  (label exit
    (db-ids-destroy ids)
    (db-data-list-data-free data)
    (db-data-list-destroy data)
    (status->scm-return result)))

(define (scm-db-intern-update scm-txn scm-id scm-data) (SCM SCM SCM SCM)
  status-init
  (define data db-data-t (struct-literal 0 0))
  (status-require! (scm->db-data scm-data (address-of data)))
  (define id db-id-t (scm->db-id scm-id))
  (status-require! (db-intern-update (scm->txn scm-txn) id data))
  (label exit
    (free (struct-get data data))
    (status->scm-return SCM-BOOL-T)))

(define (scm-db-extern-update scm-txn scm-id scm-data) (SCM SCM SCM SCM)
  status-init
  (define data db-data-t)
  (status-require! (scm->db-data scm-data (address-of data)))
  (define id db-id-t (scm->db-id scm-id))
  (status-require! (db-extern-update (scm->txn scm-txn) id data))
  (label exit
    (free (struct-get data data))
    (status->scm-return SCM-BOOL-T)))

(define (scm-db-status-description id-status id-group) (SCM SCM SCM)
  status-init
  (struct-set status
    id (scm->int id-status)
    group (scm->int id-group))
  (scm-from-latin1-string (db-status-description status)))

(define (scm-db-status-group-id->name a) (SCM SCM)
  (scm-from-latin1-symbol (db-status-group-id->name (scm->int a))))

(define (scm-db-intern-data->id scm-txn scm-data scm-every?) (SCM SCM SCM SCM)
  status-init
  (define every? boolean (optional-every? scm-every?))
  (define data db-data-list-t* 0)
  (status-require! (scm->db-data-list scm-data (address-of data)))
  (define ids db-ids-t* 0)
  (set status (db-intern-data->id (scm->txn scm-txn) data every? (address-of ids)))
  (if (= db-status-id-condition-unfulfilled status.id) status-reset
    status-require)
  (define result SCM (db-ids->scm ids))
  (label exit
    (db-ids-destroy ids)
    (db-data-list-data-free data)
    (db-data-list-destroy data)
    (status->scm-return result)))

(define (scm-db-intern-id->data scm-txn scm-ids scm-every?) (SCM SCM SCM SCM)
  status-init
  (define every? boolean (optional-every? scm-every?))
  (define ids db-ids-t* 0)
  (status-require! (scm->db-ids scm-ids (address-of ids)))
  (define data db-data-list-t* 0)
  (set status (db-intern-id->data (scm->txn scm-txn) ids every? (address-of data)))
  (if (= db-status-id-condition-unfulfilled status.id) status-reset
    status-require)
  (define result SCM (db-data-list->scm data))
  (label exit
    (db-ids-destroy ids)
    (db-data-list-destroy data)
    (status->scm-return result)))

(define (scm-db-intern-small? id-scm) (SCM SCM)
  (scm-from-bool (db-intern-small? (scm->db-id id-scm))))

(define (scm-db-intern-small-id->data id-scm) (SCM SCM)
  (db-id->scm (db-intern-small-id->data (scm->db-id id-scm))))

(define (scm-db-intern-small-data->id data-scm) (SCM SCM)
  (db-id->scm (db-intern-small-data->id (scm->db-id data-scm))))

(pre-let
  ( (result-add key struct-key)
    (scm-c-alist-add-from-struct result result-c key struct-key db-ids->scm)
    (result-add-records key struct-key)
    (scm-c-alist-add-from-struct result result-c key struct-key db-relation-records->scm))
  (define (scm-db-index-errors-intern scm-txn) (SCM SCM)
    status-init
    (define result-c db-index-errors-intern-t)
    (status-require! (db-index-errors-intern (scm->txn scm-txn) (address-of result-c)))
    (define result SCM SCM-EOL)
    (result-add "different-data-id" different-data-id)
    (result-add "excess-data-id" excess-data-id)
    (result-add "different-id-data" different-id-data)
    (result-add "missing-id-data" missing-id-data)
    (scm-c-alist-add-from-struct result result-c "errors?" errors? scm-from-bool)
    (label exit
      (status->scm-return result)))
  (define (scm-db-index-errors-extern scm-txn) (SCM SCM)
    status-init
    (define result-c db-index-errors-extern-t)
    (status-require! (db-index-errors-extern (scm->txn scm-txn) (address-of result-c)))
    (define result SCM SCM-EOL)
    (result-add "different-data-extern" different-data-extern)
    (result-add "excess-data-extern" excess-data-extern)
    (result-add "different-id-data" different-id-data)
    (result-add "missing-id-data" missing-id-data)
    (scm-c-alist-add-from-struct result result-c "errors?" errors? scm-from-bool)
    (label exit
      (status->scm-return result)))
  (define (scm-db-index-errors-relation scm-txn) (SCM SCM)
    status-init
    (define result-c db-index-errors-relation-t)
    (status-require! (db-index-errors-relation (scm->txn scm-txn) (address-of result-c)))
    (define result SCM SCM-EOL)
    (result-add-records "missing-right-left" missing-right-left)
    (result-add-records "missing-label-left" missing-label-left)
    (result-add-records "excess-right-left" excess-right-left)
    (result-add-records "excess-label-left" excess-label-left)
    (scm-c-alist-add-from-struct result result-c "errors?" errors? scm-from-bool)
    (label exit
      (status->scm-return result))))

(pre-define (define-scm-db-index-recreate name)
  (define ((pre-concat scm-db-index-recreate- name)) SCM
    status-init
    (status-require! ((pre-concat db-index-recreate- name)))
    (label exit
      (status->scm-return SCM-BOOL-T))))

(define-scm-db-index-recreate intern)
(define-scm-db-index-recreate extern)
(define-scm-db-index-recreate relation)

(define (scm-db-node-select scm-txn scm-types scm-offset) (SCM SCM SCM SCM)
  (if (scm-is-null scm-types) (return (selection->scm 0)))
  status-init
  (define offset b32 (optional-offset scm-offset))
  (define state db-node-read-state-t* (malloc (sizeof db-node-read-state-t)))
  (if (not state) (status-set-id-goto db-status-id-memory))
  (define types b8 (optional-types scm-types))
  (status-require! (db-node-select (scm->txn scm-txn) types offset state))
  (label exit
    (if (and status-failure? (not (status-id-is? db-status-id-no-more-data)))
      (begin
        (free state)
        (return (status->scm-error status))))
    (active-read-states-add! state db-read-state-type-node)
    (return (selection->scm state))))

(define (scm-db-node-read scm-selection scm-count) (SCM SCM SCM)
  status-init
  (define state db-node-read-state-t* (scm->selection scm-selection node))
  (define count b32 (optional-count scm-count))
  (define records db-data-records-t* 0)
  (db-status-require-read! (db-node-read state count (address-of records)))
  db-status-success-if-no-more-data
  (define result SCM (db-data-records->scm records db-data-record->scm))
  (label exit
    (set status.group db-status-group-lmdb)
    (db-data-records-destroy records)
    (status->scm-return result)))

(pre-define (db-status-require-malloc malloc-result)
  (if (not malloc-result) (status-set-both-goto db-status-group-dg db-status-id-memory)))

(pre-define
  (set-ordinal-match-data scm-ordinal) (define ordinal db-ordinal-match-data-t* 0)
  (if (scm-is-true (scm-list? scm-ordinal))
    (begin
      (define scm-ordinal-min SCM (scm-assoc-ref scm-ordinal (scm-from-latin1-symbol "min")))
      (define scm-ordinal-max SCM (scm-assoc-ref scm-ordinal (scm-from-latin1-symbol "max")))
      (set ordinal (calloc 1 (sizeof db-ordinal-match-data-t)))
      (db-status-require-malloc ordinal)
      (struct-pointer-set ordinal
        min
        (if* (scm-is-integer scm-ordinal-min) (scm->uint scm-ordinal-min)
          0)
        max
        (if* (scm-is-integer scm-ordinal-max) (scm->uint scm-ordinal-max)
          0)))))

(define
  (scm-db-relation-select scm-txn scm-left scm-right scm-label scm-retrieve scm-ordinal scm-offset)
  (SCM SCM SCM SCM SCM SCM SCM SCM)
  (if (or (scm-is-null scm-left) (scm-is-null scm-right) (scm-is-null scm-label))
    (return (selection->scm 0)))
  status-init
  (set-ordinal-match-data scm-ordinal)
  (define offset b32 (optional-offset scm-offset))
  (define state db-guile-relation-read-state-t* (malloc (sizeof db-guile-relation-read-state-t)))
  (db-status-require-malloc state)
  (db-define-ids-3 left right label)
  (optional-ids scm-left left)
  (optional-ids scm-right right)
  (optional-ids scm-label label)
  (status-require!
    (db-relation-select
      (scm->txn scm-txn)
      left right label ordinal offset (address-of (struct-pointer-get state db-state))))
  (define records->scm (function-pointer SCM db-relation-records-t*)
    (optional-relation-retrieve scm-retrieve))
  (if (not records->scm) (status-set-both-goto db-status-group-dg db-status-id-input-type))
  (struct-pointer-set state
    left left
    right right
    label label
    records->scm records->scm)
  (label exit
    (if status-failure?
      (begin
        (free state)
        (free left)
        (free right)
        (free label)
        (if (status-id-is? db-status-id-no-more-data) (return (selection->scm 0))
          (status->scm-error status)))
      (begin
        (active-read-states-add! state db-read-state-type-relation)
        (return (selection->scm state))))))

(define (scm-db-relation-delete scm-txn scm-left scm-right scm-label scm-ordinal)
  (SCM SCM SCM SCM SCM SCM)
  (if (or (scm-is-null scm-left) (scm-is-null scm-right) (scm-is-null scm-label))
    (return SCM-BOOL-T))
  status-init
  (set-ordinal-match-data scm-ordinal)
  (db-define-ids-3 left right label)
  (optional-ids scm-left left)
  (optional-ids scm-right right)
  (optional-ids scm-label label)
  (status-require! (db-relation-delete (scm->txn scm-txn) left right label ordinal))
  (label exit
    (status->scm-return SCM-BOOL-T)))

(define (scm-db-relation-read scm-selection scm-count) (SCM SCM SCM)
  status-init
  (define state db-guile-relation-read-state-t* (scm->selection scm-selection guile-relation))
  (if (not state) (return SCM-EOL))
  (define count b32 (optional-count scm-count))
  (define records db-relation-records-t* 0)
  (define records->scm (function-pointer SCM db-relation-records-t*)
    (struct-pointer-get state records->scm))
  (db-status-require-read!
    (db-relation-read (address-of (struct-pointer-get state db-state)) count (address-of records)))
  db-status-success-if-no-more-data
  (define result SCM (records->scm records))
  (label exit
    (db-relation-records-destroy records)
    (status->scm-return result)))

(define (scm-db-txn? a) (SCM SCM) (return (scm-from-bool (SCM-SMOB-PREDICATE scm-type-txn a))))
(define (scm-db-txn-active? a) (SCM SCM) (return (scm-from-bool (SCM-SMOB-DATA a))))

(define (scm-db-selection? a) (SCM SCM)
  (return (scm-from-bool (SCM_SMOB_PREDICATE scm-type-selection a))))

(define (scm-db-debug-count-all-btree-entries txn) (SCM SCM)
  status-init
  (define result b32)
  (status-require! (db-debug-count-all-btree-entries (scm->txn txn) (address-of result)))
  (label exit
    (status->scm-return (scm-from-uint32 result))))

(define (scm-db-debug-display-btree-counts txn) (SCM SCM)
  status-init
  (status-require! (db-debug-display-btree-counts (scm->txn txn)))
  (label exit
    (status->scm-return SCM-BOOL-T)))

(define (scm-db-debug-display-content-left->right txn) (SCM SCM)
  status-init
  (db-status-require-read! (db-debug-display-content-left->right (scm->txn txn)))
  (label exit
    (status->scm-return SCM-BOOL-T)))

(define (scm-db-debug-display-content-right->left txn) (SCM SCM)
  status-init
  (db-status-require-read! (db-debug-display-content-right->left (scm->txn txn)))
  (label exit
    (status->scm-return SCM-BOOL-T)))

(define (db-guile-init) b0
  (set
    scm-type-txn (scm-make-smob-type "db-txn" #t)
    scm-type-selection (scm-make-smob-type "db-selection" 0)
    db-scm-write (scm-variable-ref (scm-c-lookup "write"))
    db-scm-read (scm-variable-ref (scm-c-lookup "read"))
    scm-symbol-label (scm-from-latin1-symbol "label")
    scm-symbol-ordinal (scm-from-latin1-symbol "ordinal")
    scm-symbol-left (scm-from-latin1-symbol "left")
    scm-symbol-right (scm-from-latin1-symbol "right")
    scm-rnrs-raise (scm-c-public-ref "rnrs exceptions" "raise")
    scm-bytevector-null (scm-c-make-bytevector 0))
  (define m SCM (scm-c-resolve-module "sph storage dg"))
  (scm-c-module-define m "db-init-extension" SCM-EOL)
  (scm-c-module-define m "db-size-octets-id" (scm-from-size-t db-size-octets-id))
  (scm-c-module-define
    m
    "db-size-octets-data-max" (scm-from-size-t (- db-size-octets-data-max db-guile-intern-type-size)))
  (scm-c-module-define m "db-size-octets-data-min" (scm-from-size-t db-size-octets-data-min))
  (scm-c-module-define m "db-null" (scm-from-uint8 db-null))
  (scm-c-module-define m "db-type-bit-id" (scm-from-uint8 db-type-bit-id))
  (scm-c-module-define m "db-type-bit-intern" (scm-from-uint8 db-type-bit-intern))
  (scm-c-module-define m "db-type-bit-extern" (scm-from-uint8 db-type-bit-extern))
  (scm-c-module-define m "db-type-bit-intern-small" (scm-from-uint8 db-type-bit-intern-small))
  scm-c-define-procedure-c-init
  (scm-c-define-procedure-c "db-exit" 0 0 0 scm-db-exit "completely deinitialises the database")
  (scm-c-define-procedure-c "db-init" 1 1 0 scm-db-init "path [options] ->")
  (scm-c-define-procedure-c "db-id?" 1 0 0 scm-db-id? "integer -> boolean")
  (scm-c-define-procedure-c "db-intern?" 1 0 0 scm-db-intern? "integer -> boolean")
  (scm-c-define-procedure-c "db-extern?" 1 0 0 scm-db-extern? "integer -> boolean")
  (scm-c-define-procedure-c "db-relation?" 1 0 0 scm-db-relation? "integer -> boolean")
  (scm-c-define-procedure-c "db-initialised?" 0 0 0 scm-db-initialised? "-> boolean")
  (scm-c-define-procedure-c "db-root" 0 0 0 scm-db-root "-> string")
  (scm-c-define-procedure-c "db-txn-create-read" 0 0 0 scm-db-txn-create-read "-> db-txn")
  (scm-c-define-procedure-c "db-txn-create-write" 0 0 0 scm-db-txn-create-write "-> db-txn")
  (scm-c-define-procedure-c "db-txn-abort" 1 0 0 scm-db-txn-abort "db-txn ->")
  (scm-c-define-procedure-c "db-txn-commit" 1 0 0 scm-db-txn-commit "db-txn -> unspecified")
  (scm-c-define-procedure-c "db-id-create" 1 1 0 scm-db-id-create "db-txn [count] -> (integer ...)")
  (scm-c-define-procedure-c "db-identify" 2 0 0 scm-db-identify "db-txn (integer:id ...) -> list")
  (scm-c-define-procedure-c "db-exists?" 2 0 0 scm-db-exists? "db-txn (integer:id ...) -> list")
  (scm-c-define-procedure-c "db-statistics" 1 0 0 scm-db-statistics "db-txn -> alist")
  (scm-c-define-procedure-c
    "db-relation-ensure"
    3 3 0 scm-db-relation-ensure "db-txn list list [list false/procedure integer/any] -> list:ids")
  (scm-c-define-procedure-c "db-intern-ensure" 2 0 0 scm-db-intern-ensure "db-txn list -> list:ids")
  (scm-c-define-procedure-c
    "db-status-description" 2 0 0 scm-db-status-description "integer:status integer:group -> string")
  (scm-c-define-procedure-c
    "db-status-group-id->name" 1 0 0 scm-db-status-group-id->name "integer:group-id -> string")
  (scm-c-define-procedure-c
    "db-intern-id->data" 2 2 0 scm-db-intern-id->data "db-txn list [boolean:every?] -> (any ...)")
  (scm-c-define-procedure-c
    "db-intern-data->id" 2 1 0 scm-db-intern-data->id "db-txn list [boolean:every?] -> (integer ...)")
  (scm-c-define-procedure-c "db-intern-small?" 1 0 0 scm-db-intern-small? "id -> boolean")
  (scm-c-define-procedure-c
    "db-intern-small-data->id" 1 0 0 scm-db-intern-small-data->id "integer -> id")
  (scm-c-define-procedure-c
    "db-intern-small-id->data" 1 0 0 scm-db-intern-small-id->data "id -> integer")
  (scm-c-define-procedure-c "db-delete" 2 0 0 scm-db-delete "db-txn list -> unspecified")
  (scm-c-define-procedure-c
    "db-extern-create" 1 2 0 scm-db-extern-create "db-txn [integer:count any:data] -> list")
  (scm-c-define-procedure-c
    "db-extern-id->data" 2 2 0 scm-db-extern-id->data "db-txn (integer ...) [boolean:every?] -> list")
  (scm-c-define-procedure-c "db-extern-data->id" 2 0 0 scm-db-extern-data->id "db-txn any -> list")
  (scm-c-define-procedure-c
    "db-index-errors-relation" 1 0 0 scm-db-index-errors-relation "db-txn -> list")
  (scm-c-define-procedure-c
    "db-index-errors-intern" 1 0 0 scm-db-index-errors-intern "db-txn -> list")
  (scm-c-define-procedure-c
    "db-index-errors-extern" 1 0 0 scm-db-index-errors-extern "db-txn -> list")
  (scm-c-define-procedure-c "db-index-recreate-intern" 0 0 0 scm-db-index-recreate-intern "-> true")
  (scm-c-define-procedure-c "db-index-recreate-extern" 0 0 0 scm-db-index-recreate-extern "-> true")
  (scm-c-define-procedure-c
    "db-index-recreate-relation" 0 0 0 scm-db-index-recreate-relation "-> true")
  (scm-c-define-procedure-c
    "db-node-select"
    1
    2
    0
    scm-db-node-select
    "db-txn [types offset] -> db-selection
    types is zero or a combination of bits from db-type-bit-* variables, for example (logior db-type-bit-intern db-type-bit-extern)")
  (scm-c-define-procedure-c
    "db-node-read" 1 1 0 scm-db-node-read "db-selection [count] -> (vector ...)")
  (scm-c-define-procedure-c
    "db-relation-select"
    1
    6
    0
    scm-db-relation-select
    "db-txn (integer ...):left [(integer ...):right (integer ...):label symbol:retrieve-only-field list:((symbol:min integer) (symbol:max integer)):ordinal integer:offset] -> db-selection")
  (scm-c-define-procedure-c
    "db-relation-delete"
    2
    3
    0
    scm-db-relation-delete
    "db-txn (integer ...):left [(integer ...):right (integer ...):label list:((symbol:min integer) (symbol:max integer)):ordinal] -> unspecified")
  (scm-c-define-procedure-c
    "db-relation-read" 1 1 0 scm-db-relation-read "db-selection [integer:count] -> (vector ...)")
  (scm-c-define-procedure-c
    "db-intern-update" 3 0 0 scm-db-intern-update "db-txn integer:id any:data -> true")
  (scm-c-define-procedure-c
    "db-extern-update" 3 0 0 scm-db-extern-update "db-txn integer:id any:data -> true")
  (scm-c-define-procedure-c "db-selection?" 1 0 0 scm-db-selection? "any -> boolean")
  (scm-c-define-procedure-c "db-txn?" 1 0 0 scm-db-txn? "any -> boolean")
  (scm-c-define-procedure-c "db-txn-active?" 1 0 0 scm-db-txn-active? "db-txn -> boolean")
  (scm-c-define-procedure-c
    "db-debug-count-all-btree-entries" 1 0 0 scm-db-debug-count-all-btree-entries "db-txn -> integer")
  (scm-c-define-procedure-c
    "db-debug-display-btree-counts" 1 0 0 scm-db-debug-display-btree-counts "db-txn ->")
  (scm-c-define-procedure-c
    "db-debug-display-content-left->right" 1 0 0 scm-db-debug-display-content-left->right "db-txn ->")
  (scm-c-define-procedure-c
    "db-debug-display-content-right->left" 1 0 0 scm-db-debug-display-content-right->left "db-txn ->"))
)
