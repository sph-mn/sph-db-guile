/* bindings that arent part of the exported scheme api and debug features.
separate file because it is easier to start from the exported features */
/** SCM uint8_t* SCM -> unspecified */
#define scm_options_get(options, name, result) \
  result = scm_assoc_ref(scm_options, (scm_from_latin1_symbol(name))); \
  result = (scm_is_pair(result) ? scm_tail(result) : SCM_UNDEFINED)
#define scm_to_db_txn(a) ((db_txn_t*)(SCM_SMOB_DATA(a)))
#define db_txn_to_scm(pointer) \
  scm_new_smob(scm_type_txn, ((scm_t_bits)(pointer)))
#define scm_to_db_env(a) ((db_env_t*)(SCM_SMOB_DATA(a)))
#define db_env_to_scm(pointer) \
  scm_new_smob(scm_type_env, ((scm_t_bits)(pointer)))
#define scm_to_db_selection(a, selection_name) \
  ((db_##selection_name##_selection_t*)(SCM_SMOB_DATA(a)))
#define db_selection_to_scm(pointer) \
  scm_new_smob(scm_type_selection, ((scm_t_bits)(pointer)))
#define status_to_scm_error(a) \
  scm_c_error((db_status_name(a)), (db_status_description(a)))
#define scm_c_error(name, description) \
  scm_call_1(scm_rnrs_raise, \
    (scm_list_3((scm_from_latin1_symbol(name)), \
      (scm_cons((scm_from_latin1_symbol("description")), \
        (scm_from_utf8_string(description)))), \
      (scm_cons((scm_from_latin1_symbol("c-routine")), \
        (scm_from_latin1_symbol(__FUNCTION__)))))))
scm_t_bits scm_type_env;
scm_t_bits scm_type_txn;
scm_t_bits scm_type_selection;
SCM scm_rnrs_raise;