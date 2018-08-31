(define
  (scm-db-relation-ensure
    scm-txn scm-left scm-right scm-label scm-ordinal-generator scm-ordinal-generator-state)
  (SCM SCM SCM SCM SCM SCM SCM)
  status-declare
  (if (or (scm-is-undefined scm-label) (not (scm-is-true scm-label)))
    (set scm-label (scm-list-1 (scm-from-uint8 0))))
  (db-define-ids-3 left right label)
  (status-require! (scm->db-ids scm-left &left))
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

(define (scm-db-delete scm-txn scm-ids) (SCM SCM SCM)
  status-declare
  (define ids db-ids-t* 0)
  (status-require! (scm->db-ids scm-ids (address-of ids)))
  (status-require! (db-delete (scm->txn scm-txn) ids))
  (label exit
    (db-ids-destroy ids)
    (status->scm-return SCM-UNSPECIFIED)))

(define (scm-db-identify scm-txn scm-ids) (SCM SCM SCM)
  status-declare
  (define ids db-ids-t* 0)
  (status-require! (scm->db-ids scm-ids (address-of ids)))
  (define ids-result db-ids-t* 0)
  (status-require! (db-identify (scm->txn scm-txn) ids (address-of ids-result)))
  (define result SCM (db-ids->scm ids-result))
  (label exit
    (db-ids-destroy ids)
    (db-ids-destroy ids-result)
    (status->scm-return result)))

(define (scm-db-intern-ensure scm-txn scm-data) (SCM SCM SCM)
  status-declare
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
  status-declare
  (define data db-data-t (struct-literal 0 0))
  (status-require! (scm->db-data scm-data (address-of data)))
  (define id db-id-t (scm->db-id scm-id))
  (status-require! (db-intern-update (scm->txn scm-txn) id data))
  (label exit
    (free (struct-get data data))
    (status->scm-return SCM-BOOL-T)))

(define (scm-db-extern-update scm-txn scm-id scm-data) (SCM SCM SCM SCM)
  status-declare
  (define data db-data-t)
  (status-require! (scm->db-data scm-data (address-of data)))
  (define id db-id-t (scm->db-id scm-id))
  (status-require! (db-extern-update (scm->txn scm-txn) id data))
  (label exit
    (free (struct-get data data))
    (status->scm-return SCM-BOOL-T)))

(define (scm-db-intern-data->id scm-txn scm-data scm-every?) (SCM SCM SCM SCM)
  status-declare
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
  status-declare
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

(define (scm-db-record-virtual? id-scm) (SCM SCM)
  (scm-from-bool (db-record-virtual? (scm->db-id id-scm))))

(define (scm-db-record-virtual-id->data id-scm) (SCM SCM)
  (db-id->scm (db-record-virtual-id->data (scm->db-id id-scm))))

(define (scm-db-record-virtual-data->id data-scm) (SCM SCM)
  (db-id->scm (db-record-virtual-data->id (scm->db-id data-scm))))

(define-scm-db-index-recreate intern)
(define-scm-db-index-recreate extern)

(define (scm-db-record-read scm-selection scm-count) (SCM SCM SCM)
  status-declare
  (define state db-record-selection-t* (scm->selection scm-selection record))
  (define count b32 (optional-count scm-count))
  (define records db-data-records-t* 0)
  (db-status-require-read! (db-record-selection count (address-of records)))
  db-status-success-if-no-more-data
  (define result SCM (db-data-relations->scm records db-data-record->scm))
  (label exit
    (set status.group db-status-group-lmdb)
    (db-data-records-destroy records)
    (status->scm-return result)))

(define
  (scm-db-relation-select scm-txn scm-left scm-right scm-label scm-retrieve scm-ordinal scm-offset)
  (SCM SCM SCM SCM SCM SCM SCM SCM)
  (if (or (scm-is-null scm-left) (scm-is-null scm-right) (scm-is-null scm-label))
    (return (selection->scm 0)))
  status-declare
  (set-ordinal-match-data scm-ordinal)
  (define offset b32 (optional-offset scm-offset))
  (define state db-guile-relation-selection-t* (malloc (sizeof db-guile-relation-selection-t)))
  (db-status-require-malloc state)
  (db-define-ids-3 left right label)
  (optional-ids scm-left left)
  (optional-ids scm-right right)
  (optional-ids scm-label label)
  (status-require!
    (db-relation-select
      (scm->txn scm-txn)
      left right label ordinal offset (address-of (struct-pointer-get state db-state))))
  (define relations->scm (function-pointer SCM db-relations-t*)
    (optional-relation-retrieve scm-retrieve))
  (if (not relations->scm) (status-set-both-goto db-status-group-dg db-status-id-input-type))
  (struct-pointer-set state
    left left
    right right
    label label
    relations->scm relations->scm)
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
        (active-selections-add! state db-guile-selection-type-relation)
        (return (selection->scm state))))))

(define (scm-db-relation-delete scm-txn scm-left scm-right scm-label scm-ordinal)
  (SCM SCM SCM SCM SCM SCM)
  (if (or (scm-is-null scm-left) (scm-is-null scm-right) (scm-is-null scm-label))
    (return SCM-BOOL-T))
  status-declare
  (set-ordinal-match-data scm-ordinal)
  (db-define-ids-3 left right label)
  (optional-ids scm-left left)
  (optional-ids scm-right right)
  (optional-ids scm-label label)
  (status-require! (db-relation-delete (scm->txn scm-txn) left right label ordinal))
  (label exit
    (status->scm-return SCM-BOOL-T)))

(define (scm-db-relation-read scm-selection scm-count) (SCM SCM SCM)
  status-declare
  (define state db-guile-relation-selection-t* (scm->selection scm-selection guile-relation))
  (if (not state) (return SCM-EOL))
  (define count b32 (optional-count scm-count))
  (define records db-relations-t* 0)
  (define relations->scm (function-pointer SCM db-relations-t*)
    (struct-pointer-get state relations->scm))
  (db-status-require-read!
    (db-relation-read (address-of (struct-pointer-get state db-state)) count (address-of records)))
  db-status-success-if-no-more-data
  (define result SCM (relations->scm records))
  (label exit
    (db-relations-destroy records)
    (status->scm-return result)))

(define (scm-db-txn? a) (SCM SCM) (return (scm-from-bool (SCM-SMOB-PREDICATE scm-type-txn a))))

(define (db-guile-init) b0
  "map and register guile bindings"
  (set
    scm-type-txn (scm-make-smob-type "db-txn" 0)
    scm-type-selection (scm-make-smob-type "db-selection" 0)
    scm-type-env (scm-make-smob-type "db-env" 0)
    db-scm-write (scm-variable-ref (scm-c-lookup "write"))
    db-scm-read (scm-variable-ref (scm-c-lookup "read"))
    scm-symbol-label (scm-from-latin1-symbol "label")
    scm-symbol-ordinal (scm-from-latin1-symbol "ordinal")
    scm-symbol-left (scm-from-latin1-symbol "left")
    scm-symbol-right (scm-from-latin1-symbol "right")
    scm-rnrs-raise (scm-c-public-ref "rnrs exceptions" "raise")
    scm-bytevector-null (scm-c-make-bytevector 0))
  (define m SCM (scm-c-resolve-module "sph storage dg"))
  (scm-c-module-define m "db-open-extension" SCM-EOL)
  (scm-c-module-define m "db-size-octets-id" (scm-from-size-t db-size-octets-id))
  (scm-c-module-define
    m
    "db-size-octets-data-max" (scm-from-size-t (- db-size-octets-data-max db-guile-intern-type-size)))
  (scm-c-module-define m "db-size-octets-data-min" (scm-from-size-t db-size-octets-data-min))
  (scm-c-module-define m "db-null" (scm-from-uint8 db-null))
  (scm-c-module-define m "db-type-bit-id" (scm-from-uint8 db-type-bit-id))
  (scm-c-module-define m "db-type-bit-intern" (scm-from-uint8 db-type-bit-intern))
  (scm-c-module-define m "db-type-bit-extern" (scm-from-uint8 db-type-bit-extern))
  (scm-c-module-define m "db-type-bit-record-virtual" (scm-from-uint8 db-type-bit-record-virtual))
  scm-c-define-procedure-c-init
  (scm-c-define-procedure-c "db-exit" 0 0 0 scm-db-exit "completely deinitialises the database")
  (scm-c-define-procedure-c "db-open" 1 1 0 scm-db-open "path [options] ->")
  (scm-c-define-procedure-c "db-id?" 1 0 0 scm-db-id? "integer -> boolean")
  (scm-c-define-procedure-c "db-intern?" 1 0 0 scm-db-intern? "integer -> boolean")
  (scm-c-define-procedure-c "db-extern?" 1 0 0 scm-db-extern? "integer -> boolean")
  (scm-c-define-procedure-c "db-relation?" 1 0 0 scm-db-relation? "integer -> boolean")
  (scm-c-define-procedure-c "db-openialised?" 0 0 0 scm-db-openialised? "-> boolean")
  (scm-c-define-procedure-c "db-root" 0 0 0 scm-db-root "-> string")
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
  (scm-c-define-procedure-c "db-record-virtual?" 1 0 0 scm-db-record-virtual? "id -> boolean")
  (scm-c-define-procedure-c
    "db-record-virtual-data->id" 1 0 0 scm-db-record-virtual-data->id "integer -> id")
  (scm-c-define-procedure-c
    "db-record-virtual-id->data" 1 0 0 scm-db-record-virtual-id->data "id -> integer")
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
    "db-record-select"
    1
    2
    0
    scm-db-record-select
    "db-txn [types offset] -> db-selection
    types is zero or a combination of bits from db-type-bit-* variables, for example (logior db-type-bit-intern db-type-bit-extern)")
  (scm-c-define-procedure-c
    "db-record-read" 1 1 0 scm-db-record-read "db-selection [count] -> (vector ...)")
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