(sc-comment
  "bindings that arent part of the exported scheme api and debug features."
  "separate file because it is easier to start from the exported features")

(pre-define
  (scm-options-get options name result)
  (begin
    "SCM uint8_t* SCM -> unspecified"
    (set
      result (scm-assoc-ref scm-options (scm-from-latin1-symbol name))
      result
      (if* (scm-is-pair result) (scm-tail result)
        SCM-UNDEFINED)))
  ; scm types
  (scm->db-txn a) (convert-type (SCM-SMOB-DATA a) db-txn-t*)
  (db-txn->scm pointer) (scm-new-smob scm-type-txn (convert-type pointer scm-t-bits))
  (scm->db-env a) (convert-type (SCM-SMOB-DATA a) db-env-t*)
  (db-env->scm pointer) (scm-new-smob scm-type-env (convert-type pointer scm-t-bits))
  (scm->db-selection a selection-name)
  (convert-type (SCM-SMOB-DATA a) (pre-concat db_ selection-name _selection-t*))
  (db-selection->scm pointer) (scm-new-smob scm-type-selection (convert-type pointer scm-t-bits))
  ; error handling
  (status->scm-error a) (scm-c-error (db-status-name a) (db-status-description a))
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
  scm-type-env scm-t-bits
  scm-type-txn scm-t-bits
  scm-type-selection scm-t-bits
  scm-rnrs-raise SCM)

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

(pre-include "./selections.c")