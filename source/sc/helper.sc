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
      (if* (scm-is-pair scm-temp) (scm-tail scm-temp)
        SCM-UNDEFINED)))
  ; scm types
  (scm->db-txn a) (convert-type (SCM-SMOB-DATA a) db-txn-t*)
  (db-txn->scm pointer) (scm-new-smob scm-type-txn (convert-type pointer scm-t-bits))
  (scm->db-env a) (convert-type (SCM-SMOB-DATA a) db-env-t*)
  (db-env->scm pointer) (scm-new-smob scm-type-env (convert-type pointer scm-t-bits))
  (scm->db-selection a selection-name)
  (convert-type (SCM-SMOB-DATA a) (pre-concat db_ selection-name _selection-t*))
  (db-selection->scm pointer) (scm-new-smob scm-type-selection (convert-type pointer scm-t-bits)))