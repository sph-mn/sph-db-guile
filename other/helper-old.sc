(pre-define
  db-guile-type-bytevector 0
  db-guile-type-integer 1
  db-guile-type-string 2
  db-guile-type-rational 3
  db-guile-type-scheme 4
  db-guile-selection-first mi-list-first
  db-guile-selection-rest mi-list-rest
  mi-list-name-prefix db-guile-selection
  mi-list-element-t db-guile-selection-t
  (optional-ids scm-a a)
  (begin
    "append ids from scm-a to db-ids-t* list if a is not false or undefined (no filter)"
    (if (scm-is-pair scm-a) (status-require! (scm->db-ids scm-a (address-of a)))))
  (status->scm-return result) (return (status->scm result))
  (optional-count a)
  (if* (scm-is-integer a) (scm->uint a)
    0)
  (optional-count-one a)
  (if* (scm-is-integer a) (scm->uint a)
    1)
  (optional-every? a) (or (scm-is-undefined a) (scm-is-true a))
  (optional-types a)
  (if* (scm-is-integer a) (scm->uint8 a)
    0)
  (optional-offset a)
  (if* (scm-is-integer a) (scm->uint32 a)
    0)
  (db-id->scm a) (scm-from-uint a)
  (scm->db-id a) (scm->uint a)
  db-guile-scm->ordinal scm->int
  (optional-relation-retrieve a)
  (if* (scm-is-symbol a)
    (case* scm-is-eq a
      (scm-symbol-right db-relations->scm-retrieve-right)
      (scm-symbol-left db-relations->scm-retrieve-left)
      (scm-symbol-label db-relations->scm-retrieve-label)
      (scm-symbol-ordinal db-relations->scm-retrieve-ordinal)
      (else 0))
    db-relations->scm)
  (define-scm-db-txn-create name flags)
  (define ((pre-concat scm-db-txn-create_ name)) SCM
    status-init
    (define txn db-txn-t*)
    (db-mdb-status-require! (mdb-txn-begin db-mdb-env 0 flags (address-of txn)))
    (define result SCM (txn->scm txn))
    (label exit
      (if (and result status-failure?) (free txn))
      (status->scm-return result)))
  (define-scm-db-index-recreate name)
  (define ((pre-concat scm-db-index-recreate- name)) SCM
    status-init
    (status-require! ((pre-concat db-index-recreate- name)))
    (label exit
      (status->scm-return SCM-BOOL-T)))
  (db-status-require-malloc malloc-result)
  (if (not malloc-result) (status-set-both-goto db-status-group-dg db-status-id-memory))
  (set-ordinal-match-data scm-ordinal)
  (begin
    (define ordinal db-ordinal-match-data-t* 0)
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
  (scm-c-error name description)
  (scm-call-1
    scm-rnrs-raise
    (scm-list-3
      (scm-from-latin1-symbol name)
      (scm-cons (scm-from-latin1-symbol "description") (scm-from-utf8-string description))
      (scm-cons (scm-from-latin1-symbol "c-routine") (scm-from-latin1-symbol __FUNCTION__))))
  (status->scm-error a) (scm-c-error (db-status-name a) (db-status-description a))
  (status->scm result)
  (if* status-success? result
    (status->scm-error status))
  (define-db-type? name)
  (define ((pre-concat scm-dg_ name _p) id) (SCM SCM)
    (return (scm-from-bool (and (scm-is-integer id) ((pre-concat dg_ name _p) (scm->uint id))))))
  (scm-string-octet-length-uint a)
  (scm->uint (scm-product (scm-string-bytes-per-char a) (scm-string-length a)))
  (db-pre-concat-primitive a b) (pre-concat a b)
  (db-pre-concat a b) (db-pre-concat-primitive a b)
  db-scm->data-integer (db-pre-concat scm_to_ db-data-integer-type)
  db-data-integer->scm (db-pre-concat scm_from_ db-data-integer-type)
  db-data-integer-t (db-pre-concat db-data-integer-type _t)
  (scm-c-alist-add-from-struct target source key struct-key convert)
  (set target
    (scm-acons (scm-from-latin1-symbol key) (convert (struct-get source struct-key)) target))
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
      a))
  (define-db-relations->scm-retrieve field-name)
  (define ((pre-concat db-relations->scm-retrieve_ field-name) a) (SCM db-relations-t*)
    (define result SCM SCM-EOL)
    (define record db-relation-record-t)
    (while a
      (set
        record (db-relations-first a)
        result (scm-cons (db-id->scm (struct-get record field-name)) result)
        a (db-relations-rest a)))
    (return result)))

(declare
  scm-type-selection scm-t-bits
  scm-type-txn scm-t-bits
  scm-type-env scm-t-bits
  db-scm-write SCM
  db-scm-read SCM
  scm-rnrs-raise SCM
  scm-symbol-label SCM
  scm-symbol-left SCM
  scm-symbol-right SCM
  scm-symbol-ordinal SCM
  scm-bytevector-null SCM
  db-guile-selection-type-t
  (type (enum (db-guile-selection-type-relation db-guile-selection-type-node)))
  db-guile-relation-selection-t
  (type
    (struct
      (left db-ids-t)
      (right db-ids-t)
      (label db-ids-t)
      (relations->scm (function-pointer SCM db-relations-t))
      (selection db-graph-selection-t)))
  db-guile-selection-t
  (type
    (struct
      (data b0*)
      (selection-type db-guile-selection-type-t))))

(define-scm-db-txn-create read MDB-RDONLY)
(define-scm-db-txn-create write 0)
(define-db-type? id)
(define-db-type? intern)
(define-db-type? extern)
(define-db-type? relation)
(define db-guile-active-selections (__thread db-guile-selection-t*) 0)
(sc-comment "depends on previously defined macro variables and types")
(pre-include "sph/mi-list")
(define (scm-db-initialised?) SCM (return (scm-from-bool db-initialised)))
(define (scm-db-root) SCM (return (scm-from-locale-string db-root)))

(define (db-guile-selection-register selection selection-type)
  "create a db-guile-selection object with the given db-selection and
  add it to db-guile-active-selections"
  (declare selection-temp db-guile-selection-t*)
  (define selection db-guile-selection-t (struct-literal state selection-type))
  (set
    selection selection-temp (db-guile-selection-add active-selections selection))
  (if selection-temp (set active-selections selection-temp)
    (status-set-both db-status-group-dg db-status-id-memory)))

(define (db-guile-selections-free) b0
  "finalise all selections of the current thread for garbage collection.
  there can only be one transaction per thread per sph-db requirements"
  (define selection db-guile-selection-t)
  (while active-selections
    (set selection (db-guile-selection-first active-selections))
    (case = (struct-get selection selection-type)
      (db-guile-selection-type-relation
        (define state db-guile-relation-selection-t* (struct-get selection state))
        (db-relation-selection-destroy (address-of (struct-pointer-get state db-state)))
        (db-ids-destroy (struct-pointer-get state left))
        (db-ids-destroy (struct-pointer-get state label))
        (db-ids-destroy (struct-pointer-get state right)) (free state))
      (db-guile-selection-type-node
        (define state db-node-selection-t* (struct-get selection state))
        (db-node-selection-destroy state)))
    (set active-selections (db-guile-selection-drop active-selections))))

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

(define (db-data-relations->scm a convert-data)
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

(define (db-relations->scm a) (SCM db-relations-t*)
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

(define-db-relations->scm-retrieve left)
(define-db-relations->scm-retrieve right)
(define-db-relations->scm-retrieve label)
(define-db-relations->scm-retrieve ordinal)

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

(define (db-guile-ordinal-generator state) (db-ordinal-t b0*)
  (define scm-state SCM (deref (convert-type state SCM*)))
  (define scm-generator SCM (scm-first scm-state))
  (define scm-result SCM (scm-apply-0 scm-generator (scm-tail scm-state)))
  (set (deref (convert-type state SCM*)) (scm-cons scm-generator scm-result))
  (return (db-guile-scm->ordinal (scm-first scm-result))))

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