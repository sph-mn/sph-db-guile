/* bindings that arent part of the exported scheme api and debug features.
separate file because it is easier to start from the exported features */
/** SCM uint8_t* SCM -> unspecified */
#define scm_options_get(options, name, result) \
  result = scm_assoc_ref(scm_options, (scm_from_latin1_symbol(name))); \
  result = (scm_is_pair(scm_temp) ? scm_tail(scm_temp) : SCM_UNDEFINED)
#define scm_to_db_txn(a) ((db_txn_t*)(SCM_SMOB_DATA(a)))
#define db_txn_to_scm(pointer) \
  scm_new_smob(scm_type_txn, ((scm_t_bits)(pointer)))
#define scm_to_db_env(a) ((db_env_t*)(SCM_SMOB_DATA(a)))
#define db_env_to_scm(pointer) \
  scm_new_smob(scm_type_env, ((scm_t_bits)(pointer)))
#define scm_to_db_selection(a, selection_name) \
  ((db_##selection_name##_selection_t*)(SCM_SMOB_DATA(a)))
#define db_selection_to_scm(pointer) \
  scm_new_smob(scm_type_selection, ((scm_t_bits)(pointer)));