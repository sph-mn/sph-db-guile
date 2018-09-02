/* bindings that arent part of the exported scheme api and debug features.
separate file because it is easier to start from the exported features */
/** SCM uint8_t* SCM -> unspecified */
#define scm_options_get(options, name, result) \
  result = scm_assoc_ref(scm_options, (scm_from_latin1_symbol(name))); \
  result = (scm_is_pair(result) ? scm_tail(result) : SCM_UNDEFINED)
#define db_env_to_scm(pointer) scm_make_foreign_object_1(scm_type_env, pointer)
#define db_txn_to_scm(pointer) scm_make_foreign_object_1(scm_type_txn, pointer)
#define db_index_to_scm(pointer) \
  scm_make_foreign_object_1(scm_type_index, pointer)
#define db_type_to_scm(pointer) \
  scm_make_foreign_object_1(scm_type_type, pointer)
#define db_selection_to_scm(pointer) \
  scm_make_foreign_object_1(scm_type_selection, pointer)
#define scm_to_db_env(a) ((db_env_t*)(scm_foreign_object_ref(a, 0)))
#define scm_to_db_txn(a) ((db_txn_t*)(scm_foreign_object_ref(a, 0)))
#define scm_to_db_index(a) ((db_index_t*)(scm_foreign_object_ref(a, 0)))
#define scm_to_db_type(a) ((db_type_t*)(scm_foreign_object_ref(a, 0)))
#define scm_to_db_selection(a, selection_name) \
  ((db_##selection_name##_selection_t*)(scm_foreign_object_ref(a, 0)))
#define status_to_scm_error(a) \
  scm_c_error((db_status_name(a)), (db_status_description(a)))
#define scm_c_error(name, description) \
  scm_call_1(scm_rnrs_raise, \
    (scm_list_3((scm_from_latin1_symbol(name)), \
      (scm_cons((scm_from_latin1_symbol("description")), \
        (scm_from_utf8_string(description)))), \
      (scm_cons((scm_from_latin1_symbol("c-routine")), \
        (scm_from_latin1_symbol(__FUNCTION__)))))))
#define status_to_scm_return(result) return ((status_to_scm(result)))
#define status_to_scm(result) \
  (status_is_success ? result : status_to_scm_error(status))
SCM scm_type_env;
SCM scm_type_txn;
SCM scm_type_selection;
SCM scm_type_type;
SCM scm_type_index;
SCM scm_rnrs_raise;
/** -> ((key . value) ...) */
SCM scm_from_mdb_stat(MDB_stat a) {
  SCM b;
  b = SCM_EOL;
  b = scm_acons(
    (scm_from_latin1_symbol("ms-entries")), (scm_from_uint((a.ms_entries))), b);
  b = scm_acons(
    (scm_from_latin1_symbol("ms-psize")), (scm_from_uint((a.ms_psize))), b);
  b = scm_acons(
    (scm_from_latin1_symbol("ms-depth")), (scm_from_uint((a.ms_depth))), b);
  b = scm_acons((scm_from_latin1_symbol("ms-branch-pages")),
    (scm_from_uint((a.ms_branch_pages))),
    b);
  b = scm_acons((scm_from_latin1_symbol("ms-leaf-pages")),
    (scm_from_uint((a.ms_leaf_pages))),
    b);
  b = scm_acons((scm_from_latin1_symbol("ms-overflow-pages")),
    (scm_from_uint((a.ms_overflow_pages))),
    b);
  return (b);
};
#include "./selections.c"
