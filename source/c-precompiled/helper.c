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
#define db_type_to_scm(pointer, env) \
  scm_make_foreign_object_2(scm_type_type, pointer, env)
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
SCM scm_symbol_binary;
SCM scm_symbol_string;
SCM scm_symbol_float32;
SCM scm_symbol_float64;
SCM scm_symbol_int8;
SCM scm_symbol_int16;
SCM scm_symbol_int32;
SCM scm_symbol_int64;
SCM scm_symbol_uint8;
SCM scm_symbol_uint16;
SCM scm_symbol_uint32;
SCM scm_symbol_uint64;
SCM scm_symbol_string8;
SCM scm_symbol_string16;
SCM scm_symbol_string32;
SCM scm_symbol_string64;
db_field_type_t scm_to_db_field_type(SCM a) {
  if (scm_is_eq(scm_symbol_binary, a)) {
    return (1);
  } else if (scm_is_eq(scm_symbol_string, a)) {
    return (3);
  } else if (scm_is_eq(scm_symbol_float32, a)) {
    return (4);
  } else if (scm_is_eq(scm_symbol_float64, a)) {
    return (6);
  } else if (scm_is_eq(scm_symbol_int16, a)) {
    return (80);
  } else if (scm_is_eq(scm_symbol_int32, a)) {
    return (112);
  } else if (scm_is_eq(scm_symbol_int64, a)) {
    return (144);
  } else if (scm_is_eq(scm_symbol_int8, a)) {
    return (48);
  } else if (scm_is_eq(scm_symbol_uint8, a)) {
    return (32);
  } else if (scm_is_eq(scm_symbol_uint16, a)) {
    return (64);
  } else if (scm_is_eq(scm_symbol_uint32, a)) {
    return (96);
  } else if (scm_is_eq(scm_symbol_uint64, a)) {
    return (128);
  } else if (scm_is_eq(scm_symbol_string8, a)) {
    return (34);
  } else if (scm_is_eq(scm_symbol_string16, a)) {
    return (66);
  } else if (scm_is_eq(scm_symbol_string32, a)) {
    return (98);
  } else if (scm_is_eq(scm_symbol_string64, a)) {
    return (130);
  } else {
    return (0);
  };
};
SCM db_field_type_to_scm(db_field_type_t a) {
  if (1 == a) {
    return (scm_symbol_binary);
  } else if (3 == a) {
    return (scm_symbol_string);
  } else if (4 == a) {
    return (scm_symbol_float32);
  } else if (6 == a) {
    return (scm_symbol_float64);
  } else if (80 == a) {
    return (scm_symbol_int16);
  } else if (112 == a) {
    return (scm_symbol_int32);
  } else if (144 == a) {
    return (scm_symbol_int64);
  } else if (48 == a) {
    return (scm_symbol_int8);
  } else if (32 == a) {
    return (scm_symbol_uint8);
  } else if (64 == a) {
    return (scm_symbol_uint16);
  } else if (96 == a) {
    return (scm_symbol_uint32);
  } else if (128 == a) {
    return (scm_symbol_uint64);
  } else if (34 == a) {
    return (scm_symbol_string8);
  } else if (66 == a) {
    return (scm_symbol_string16);
  } else if (98 == a) {
    return (scm_symbol_string32);
  } else if (130 == a) {
    return (scm_symbol_string64);
  } else {
    return (SCM_BOOL_F);
  };
};
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
