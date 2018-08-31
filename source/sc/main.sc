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

  )