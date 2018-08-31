(sc-comment
  "sph-db-guile basically registers scheme procedures that when called execute specific c-functions that manage calls to sph-db")

(pre-include "libguile.h" "sph-db.h" "./foreign/sph/one.c" "./foreign/sph/guile.c" "./helper.c")
(define (scm-db-env? a) (SCM SCM) (return (scm-from-bool (SCM_SMOB_PREDICATE scm-type-env a))))
(define (scm-db-txn? a) (SCM SCM) (return (scm-from-bool (SCM_SMOB_PREDICATE scm-type-txn a))))
(define (scm-db-env-open? a) (SCM SCM) (return (scm-from-bool (: (scm->db-env a) is-open))))
(define (scm-db-env-root a) (SCM SCM) (return (scm-from-locale-string (: (scm->db-env a) root))))

(define (scm-db-env-maxkeysize a) (SCM SCM)
  (return (scm-from-uint32 (: (scm->db-env a) maxkeysize))))

(define (scm-db-env-format a) (SCM SCM) (return (scm-from-uint32 (: (scm->db-env a) format))))

(define (scm-db-selection? a) (SCM SCM)
  (return (scm-from-bool (SCM_SMOB_PREDICATE scm-type-selection a))))

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
    root (scm->locale-string scm-root))
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
  (set txn (scm->txn scm-txn))
  (db-txn-abort &txn)
  (free txn)
  (SCM-SET-SMOB-DATA scm-txn 0)
  (return SCM-UNSPECIFIED))

(define (scm-db-txn-commit scm-txn) (SCM SCM)
  "note that commit frees cursors. db-guile-selections-free closes cursors.
  if db-guile-selections-free is called after db-txn-commit a double free occurs"
  status-declare
  (db-guile-selections-free)
  (declare txn db-txn-t*)
  (set txn (scm->txn scm-txn))
  (status-require (db-txn-commit &txn))
  (free txn)
  (SCM-SET-SMOB-DATA scm-txn 0)
  (label exit
    (status->scm-return SCM-UNSPECIFIED)))

(define (scm-db-txn-active? a) (SCM SCM) (return (scm-from-bool (SCM-SMOB-DATA a))))

(define (scm-db-txn-begin) SCM
  status-declare
  (declare txn db-txn-t*)
  (set txn 0)
  (db-calloc txn 1 (sizeof db-txn-t))
  (status-require (db-txn-begin txn))
  (label exit
    (if status-is-success (return (db-txn->scm txn))
      (begin
        (free txn)
        (status->scm-error status)
        (return SCM-UNSPECIFIED)))))

(define (scm-db-txn-write-begin) SCM
  status-declare
  (declare txn db-txn-t*)
  (set txn 0)
  (db-calloc txn 1 (sizeof db-txn-t))
  (status-require (db-txn-begin-write txn))
  (label exit
    (if status-is-success (return (db-txn->scm txn))
      (begin
        (free txn)
        (status->scm-error status)
        (return SCM-UNSPECIFIED)))))

(define (scm-db-status-description id-status id-group) (SCM SCM SCM)
  status-declare
  (struct-set status
    id (scm->int id-status)
    group (scm->int id-group))
  (scm-from-latin1-string (db-status-description status)))

(define (scm-db-status-group-id->name a) (SCM SCM)
  (scm-from-latin1-symbol (db-status-group-id->name (scm->int a))))

(define (scm-db-record-select scm-txn scm-types scm-offset) (SCM SCM SCM SCM)
  (if (scm-is-null scm-types) (return (selection->scm 0)))
  status-declare
  (define offset b32 (optional-offset scm-offset))
  (define state db-record-selection-t* (malloc (sizeof db-record-selection-t)))
  (if (not state) (status-set-id-goto db-status-id-memory))
  (define types b8 (optional-types scm-types))
  (status-require! (db-record-select (scm->txn scm-txn) types offset state))
  (label exit
    (if (and status-failure? (not (status-id-is? db-status-id-no-more-data)))
      (begin
        (free state)
        (return (status->scm-error status))))
    (active-selections-add! state db-guile-selection-type-record)
    (return (selection->scm state))))

(define (db-guile-init) void
  "prepare scm valuaes and register guile bindings"
  (set
    scm-type-txn (scm-make-smob-type "db-txn" 0)
    scm-type-env (scm-make-smob-type "db-env" 0)
    scm-type-selection (scm-make-smob-type "db-selection" 0)
    scm-rnrs-raise (scm-c-public-ref "rnrs exceptions" "raise"))
  ; exports
  scm-c-define-procedure-c-init
  (scm-c-define-procedure-c
    "db-open" 1 1 0 scm-db-open "string:root [((key . value) ...):options] ->")
  (scm-c-define-procedure-c "db-close" 1 0 0 scm-db-close "deinitialises the database handle")
  (scm-c-define-procedure-c "db-env?" 1 0 0 scm-db-env? "")
  (scm-c-define-procedure-c "db-txn?" 1 0 0 scm-db-txn? "")
  (scm-c-define-procedure-c "db-selection?" 1 0 0 scm-db-selection? "")
  (scm-c-define-procedure-c "db-env-open?" 1 0 0 scm-db-env-open? "")
  (scm-c-define-procedure-c "db-env-maxkeysize" 1 0 0 scm-db-env-maxkeysize "")
  (scm-c-define-procedure-c "db-env-root" 1 0 0 scm-db-env-root "")
  (scm-c-define-procedure-c "db-env-format" 1 0 0 scm-db-env-format "")
  (scm-c-define-procedure-c "db-statistics" 1 0 0 scm-db-statistics "")
  (scm-c-define-procedure-c "db-txn-begin" 0 0 0 scm-db-txn-begin "-> db-txn")
  (scm-c-define-procedure-c "db-txn-write-begin" 0 0 0 scm-db-txn-write-begin "-> db-txn")
  (scm-c-define-procedure-c "db-txn-abort" 1 0 0 scm-db-txn-abort "db-txn -> unspecified")
  (scm-c-define-procedure-c "db-txn-commit" 1 0 0 scm-db-txn-commit "db-txn -> unspecified")
  (scm-c-define-procedure-c "db-txn-active?" 1 0 0 scm-db-txn-active? "db-txn -> boolean"))