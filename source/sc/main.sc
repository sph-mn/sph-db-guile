(sc-comment
  "sph-db-guile basically registers scheme procedures that when called execute specific c-functions that manage calls to sph-db")

(pre-include "libguile.h" "sph-db.h" "./foreign/sph/one.c" "./forein/sph/guile.c" "./helper.c")
(define (scm-db-env? a) (SCM SCM) (return (scm-from-bool (SCM_SMOB_PREDICATE scm-type-env a))))
(define (scm-db-txn? a) (SCM SCM) (return (scm-from-bool (SCM_SMOB_PREDICATE scm-type-txn a))))

(define (scm-db-selection? a) (SCM SCM)
  (return (scm-from-bool (SCM_SMOB_PREDICATE scm-type-selection a))))

(define (scm-db-open scm-root scm-options) (SCM SCM SCM)
  status-declare
  (db-env-declare env)
  (declare
    options db-open-options-t
    options-pointer db-open-options-t*
    scm-temp SCM
    root uint8_t)
  (set
    root 0
    root (scm->locale-string scm-root))
  (status-require (db-env-new &env))
  (if (or (scm-is-undefined scm-options) (scm-is-null scm-options)) (set options-pointer 0)
    (begin
      (db-open-options-set-defaults &options)
      (scm-options-get scm-options "read-only?" scm-temp)
      (if (scm-is-bool scm-temp) (set options.read-only? (scm-is-true scm-temp)))
      (scm-options-get "maximum-size-octets")
      (if (scm-is-integer scm-temp) (set options.maximum-size-octets (scm->uint scm-temp)))
      (scm-options-get "maximum-reader-count")
      (if (scm-is-integer scm-temp) (set options.maximum-reader-count (scm->uint scm-temp)))
      (scm-options-get "filesystem-has-ordered-writes?")
      (if (scm-is-bool scm-temp)
        (set options.filesystem-has-ordered-writes? (scm-is-true scm-temp)))
      (scm-options-get "env-open-flags")
      (if (scm-is-integer scm-temp) (set options.env-open-flags (scm->uint scm-temp)))
      (scm-options-get "file-permissions")
      (if (scm-is-integer scm-temp) (set options.file-permissions (scm->uint scm-temp)))
      (set options-pointer &options)))
  (set status (db-open root options-pointer &env))
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

(define (db-guile-init) b0
  "map and register guile bindings"
  (set
    scm-type-txn (scm-make-smob-type "db-txn" 0)
    scm-type-env (scm-make-smob-type "db-env" 0)
    scm-type-selection (scm-make-smob-type "db-selection" 0))
  (scm-c-define-procedure-c
    "db-open" 1 1 0 scm-db-open "string:root [((key . value) ...):options] ->")
  (scm-c-define-procedure-c "db-close" 1 0 0 scm-db-exit "deinitialises the database handle"))