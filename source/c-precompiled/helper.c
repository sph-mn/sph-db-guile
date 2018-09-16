/* bindings that arent part of the exported scheme api and debug features.
separate file because it is easier to start from the exported features */
enum {
  status_id_field_name_not_found,
  status_id_field_value_invalid,
  status_id_invalid_argument
};
/** SCM uint8_t* SCM -> unspecified
    get value for field with name from options alist and set result
    to it or undefined if it doesnt exist */
#define scm_options_get(options, name, result) \
  result = scm_assoc_ref(scm_options, (scm_from_latin1_symbol(name))); \
  result = (scm_is_pair(result) ? scm_tail(result) : SCM_UNDEFINED)
#define define_scm_from_db_relations_retrieve(field_name) \
  SCM scm_from_db_relations_retrieve_##field_name(db_relations_t a) { \
    SCM b; \
    db_relation_t record; \
    b = SCM_EOL; \
    while (db_relations_in_range(a)) { \
      record = db_relations_get(a); \
      b = scm_cons((scm_from_uint((record.field_name))), b); \
      db_relations_forward(a); \
    }; \
    return (b); \
  }
#define scm_from_db_env(pointer) \
  scm_make_foreign_object_1(scm_type_env, pointer)
#define scm_from_db_txn(pointer) \
  scm_make_foreign_object_1(scm_type_txn, pointer)
#define scm_from_db_index(pointer) \
  scm_make_foreign_object_1(scm_type_index, pointer)
#define scm_from_db_type(pointer) \
  scm_make_foreign_object_1(scm_type_type, pointer)
#define scm_from_db_selection(pointer) \
  scm_make_foreign_object_1(scm_type_selection, pointer)
#define scm_from_db_record(pointer) \
  scm_make_foreign_object_1(scm_type_record, pointer)
#define scm_to_db_record(a) ((db_record_t*)(scm_foreign_object_ref(a, 0)))
#define scm_to_db_env(a) ((db_env_t*)(scm_foreign_object_ref(a, 0)))
#define scm_to_db_txn(a) ((db_txn_t*)(scm_foreign_object_ref(a, 0)))
#define scm_to_db_index(a) ((db_index_t*)(scm_foreign_object_ref(a, 0)))
#define scm_to_db_type(a) ((db_type_t*)(scm_foreign_object_ref(a, 0)))
#define scm_to_db_selection(a) scm_foreign_object_ref(a, 0)
#define status_group_db_guile "db-guile"
#define scm_from_status_error(a) \
  scm_c_error( \
    (a.group), (db_guile_status_name(a)), (db_guile_status_description(a)))
#define scm_c_error(group, name, description) \
  scm_call_1(scm_rnrs_raise, \
    (scm_list_4((scm_from_latin1_symbol(group)), \
      (scm_from_latin1_symbol(name)), \
      (scm_cons((scm_from_latin1_symbol("description")), \
        (scm_from_utf8_string(description)))), \
      (scm_cons((scm_from_latin1_symbol("c-routine")), \
        (scm_from_latin1_symbol(__FUNCTION__)))))))
#define scm_from_status_return(result) return ((scm_from_status(result)))
#define scm_from_status(result) \
  (status_is_success ? result : scm_from_status_error(status))
SCM scm_rnrs_raise;
SCM scm_symbol_binary8;
SCM scm_symbol_binary16;
SCM scm_symbol_binary32;
SCM scm_symbol_binary64;
SCM scm_symbol_string64;
SCM scm_symbol_string32;
SCM scm_symbol_string16;
SCM scm_symbol_string8;
SCM scm_symbol_binary8f;
SCM scm_symbol_binary16f;
SCM scm_symbol_binary32f;
SCM scm_symbol_binary64f;
SCM scm_symbol_float32f;
SCM scm_symbol_float64f;
SCM scm_symbol_int16f;
SCM scm_symbol_int32f;
SCM scm_symbol_int64f;
SCM scm_symbol_int8f;
SCM scm_symbol_min;
SCM scm_symbol_max;
SCM scm_symbol_label;
SCM scm_symbol_left;
SCM scm_symbol_ordinal;
SCM scm_symbol_right;
SCM scm_symbol_string16f;
SCM scm_symbol_string32f;
SCM scm_symbol_string64f;
SCM scm_symbol_string8f;
SCM scm_symbol_uint16f;
SCM scm_symbol_uint32f;
SCM scm_symbol_uint64f;
SCM scm_symbol_uint8f;
SCM scm_symbol_uint128f;
SCM scm_symbol_uint256f;
SCM scm_symbol_int128f;
SCM scm_symbol_int256f;
SCM scm_symbol_string128f;
SCM scm_symbol_string256f;
SCM scm_symbol_binary128f;
SCM scm_symbol_binary256f;
SCM scm_type_env;
SCM scm_type_index;
SCM scm_type_record;
SCM scm_type_selection;
SCM scm_type_txn;
SCM scm_type_type;
define_scm_from_db_relations_retrieve(left);
define_scm_from_db_relations_retrieve(right);
define_scm_from_db_relations_retrieve(label);
define_scm_from_db_relations_retrieve(ordinal);
/** get the db-field for either a field offset integer or field name */
status_t
scm_to_field_offset(SCM scm_a, db_type_t* type, db_fields_len_t* result) {
  status_declare;
  db_field_t* field;
  uint8_t* field_name;
  field_name = 0;
  if (scm_is_integer(scm_a)) {
    *result = scm_to_uintmax(scm_a);
  } else {
    field_name = scm_to_utf8_stringn(scm_a, 0);
    field = db_type_field_get(type, field_name);
    free(field_name);
    if (field) {
      *result = field->offset;
    } else {
      status_set_both_goto(
        status_group_db_guile, status_id_field_name_not_found);
    };
  };
exit:
  return (status);
};
/** memory for result is handled by gc */
status_t scm_to_field_offsets(SCM scm_type,
  SCM scm_fields,
  db_fields_len_t** result,
  db_fields_len_t* result_len) {
  status_declare;
  SCM scm_field;
  db_field_t* field;
  db_fields_len_t i;
  db_fields_len_t fields_len;
  db_fields_len_t* fields;
  uint8_t* field_name;
  db_type_t* type;
  fields_len = scm_to_uintmax((scm_length(scm_fields)));
  type = scm_to_db_type(scm_type);
  fields = scm_gc_calloc((fields_len * sizeof(db_fields_len_t)), "fields");
  for (i = 0; (i < fields_len);
       i = (1 + i), scm_fields = scm_tail(scm_fields)) {
    scm_field = scm_first(scm_fields);
    status_require(
      (scm_to_field_offset((scm_first(scm_fields)), type, (i + fields))));
  };
  *result = fields;
  *result_len = fields_len;
exit:
  return (status);
};
/** get the description if available for a status */
uint8_t* db_guile_status_description(status_t a) {
  char* b;
  if (!strcmp(status_group_db_guile, (a.group))) {
    if (status_id_field_name_not_found == a.id) {
      b = "no field found with given name";
    } else if (status_id_field_value_invalid == a.id) {
      b = "field value invalid";
    } else if (status_id_invalid_argument == a.id) {
      b = "invalid argument";
    } else {
      b = "";
    };
  } else {
    b = db_status_description(a);
  };
  return (((uint8_t*)(b)));
};
/** get the name if available for a status */
uint8_t* db_guile_status_name(status_t a) {
  char* b;
  if (!strcmp(status_group_db_guile, (a.group))) {
    if (status_id_field_name_not_found == a.id) {
      b = "field-not-found";
    } else if (status_id_field_value_invalid == a.id) {
      b = "field-value-invalid";
    } else if (status_id_invalid_argument == a.id) {
      b = "invalid-argument";
    } else {
      b = "unknown";
    };
  } else {
    b = db_status_name(a);
  };
  return (((uint8_t*)(b)));
};
/** float32 not supported by guile */
db_field_type_t scm_to_db_field_type(SCM a) {
  if (scm_is_eq(scm_symbol_string8, a)) {
    return (db_field_type_string8);
  } else if (scm_is_eq(scm_symbol_string16, a)) {
    return (db_field_type_string16);
  } else if (scm_is_eq(scm_symbol_string32, a)) {
    return (db_field_type_string32);
  } else if (scm_is_eq(scm_symbol_string64, a)) {
    return (db_field_type_string64);
  } else if (scm_is_eq(scm_symbol_binary8, a)) {
    return (db_field_type_binary8);
  } else if (scm_is_eq(scm_symbol_binary16, a)) {
    return (db_field_type_binary16);
  } else if (scm_is_eq(scm_symbol_binary32, a)) {
    return (db_field_type_binary32);
  } else if (scm_is_eq(scm_symbol_binary64, a)) {
    return (db_field_type_binary64);
  } else if (scm_is_eq(scm_symbol_binary8f, a)) {
    return (db_field_type_binary8f);
  } else if (scm_is_eq(scm_symbol_binary16f, a)) {
    return (db_field_type_binary16f);
  } else if (scm_is_eq(scm_symbol_binary32f, a)) {
    return (db_field_type_binary32f);
  } else if (scm_is_eq(scm_symbol_binary64f, a)) {
    return (db_field_type_binary64f);
  } else if (scm_is_eq(scm_symbol_binary128f, a)) {
    return (db_field_type_binary128f);
  } else if (scm_is_eq(scm_symbol_binary256f, a)) {
    return (db_field_type_binary256f);
  } else if (scm_is_eq(scm_symbol_uint8f, a)) {
    return (db_field_type_uint8f);
  } else if (scm_is_eq(scm_symbol_uint16f, a)) {
    return (db_field_type_uint16f);
  } else if (scm_is_eq(scm_symbol_uint32f, a)) {
    return (db_field_type_uint32f);
  } else if (scm_is_eq(scm_symbol_uint64f, a)) {
    return (db_field_type_uint64f);
  } else if (scm_is_eq(scm_symbol_uint128f, a)) {
    return (db_field_type_uint128f);
  } else if (scm_is_eq(scm_symbol_uint256f, a)) {
    return (db_field_type_uint256f);
  } else if (scm_is_eq(scm_symbol_int8f, a)) {
    return (db_field_type_int8f);
  } else if (scm_is_eq(scm_symbol_int16f, a)) {
    return (db_field_type_int16f);
  } else if (scm_is_eq(scm_symbol_int32f, a)) {
    return (db_field_type_int32f);
  } else if (scm_is_eq(scm_symbol_int64f, a)) {
    return (db_field_type_int64f);
  } else if (scm_is_eq(scm_symbol_float64f, a)) {
    return (db_field_type_float64f);
  } else if (scm_is_eq(scm_symbol_string8f, a)) {
    return (db_field_type_string8f);
  } else if (scm_is_eq(scm_symbol_string16f, a)) {
    return (db_field_type_string16f);
  } else if (scm_is_eq(scm_symbol_string32f, a)) {
    return (db_field_type_string32f);
  } else if (scm_is_eq(scm_symbol_string64f, a)) {
    return (db_field_type_string64f);
  } else if (scm_is_eq(scm_symbol_string128f, a)) {
    return (db_field_type_string128f);
  } else if (scm_is_eq(scm_symbol_string256f, a)) {
    return (db_field_type_string256f);
  } else if (scm_is_eq(scm_symbol_int128f, a)) {
    return (db_field_type_int128f);
  } else if (scm_is_eq(scm_symbol_int256f, a)) {
    return (db_field_type_int256f);
  } else {
    return (0);
  };
};
SCM scm_from_db_field_type(db_field_type_t a) {
  if (db_field_type_string8 == a) {
    return (scm_symbol_string8);
  } else if (db_field_type_string16 == a) {
    return (scm_symbol_string16);
  } else if (db_field_type_string32 == a) {
    return (scm_symbol_string32);
  } else if (db_field_type_string64 == a) {
    return (scm_symbol_string64);
  } else if (db_field_type_binary8 == a) {
    return (scm_symbol_binary8);
  } else if (db_field_type_binary16 == a) {
    return (scm_symbol_binary16);
  } else if (db_field_type_binary32 == a) {
    return (scm_symbol_binary32);
  } else if (db_field_type_binary64 == a) {
    return (scm_symbol_binary64);
  } else if (db_field_type_binary8f == a) {
    return (scm_symbol_binary8f);
  } else if (db_field_type_binary16f == a) {
    return (scm_symbol_binary16f);
  } else if (db_field_type_binary32f == a) {
    return (scm_symbol_binary32f);
  } else if (db_field_type_binary64f == a) {
    return (scm_symbol_binary64f);
  } else if (db_field_type_uint8f == a) {
    return (scm_symbol_uint8f);
  } else if (db_field_type_uint16f == a) {
    return (scm_symbol_uint16f);
  } else if (db_field_type_uint32f == a) {
    return (scm_symbol_uint32f);
  } else if (db_field_type_uint64f == a) {
    return (scm_symbol_uint64f);
  } else if (db_field_type_int8f == a) {
    return (scm_symbol_int8f);
  } else if (db_field_type_int16f == a) {
    return (scm_symbol_int16f);
  } else if (db_field_type_int32f == a) {
    return (scm_symbol_int32f);
  } else if (db_field_type_int64f == a) {
    return (scm_symbol_int64f);
  } else if (db_field_type_string8f == a) {
    return (scm_symbol_string8f);
  } else if (db_field_type_string16f == a) {
    return (scm_symbol_string16f);
  } else if (db_field_type_string32f == a) {
    return (scm_symbol_string32f);
  } else if (db_field_type_string64f == a) {
    return (scm_symbol_string64f);
  } else if (db_field_type_float64f == a) {
    return (scm_symbol_float64f);
  } else if (db_field_type_string128f == a) {
    return (scm_symbol_string128f);
  } else if (db_field_type_string256f == a) {
    return (scm_symbol_string256f);
  } else if (db_field_type_int128f == a) {
    return (scm_symbol_int128f);
  } else if (db_field_type_int256f == a) {
    return (scm_symbol_int256f);
  } else if (db_field_type_uint128f == a) {
    return (scm_symbol_uint128f);
  } else if (db_field_type_uint256f == a) {
    return (scm_symbol_uint256f);
  } else if (db_field_type_binary128f == a) {
    return (scm_symbol_binary128f);
  } else if (db_field_type_binary256f == a) {
    return (scm_symbol_binary256f);
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
/** db-index-t* -> SCM:((field-offset . field-name) ...) */
SCM scm_from_db_index_fields(db_index_t* a) {
  db_field_t field;
  db_type_t* type;
  db_fields_len_t i;
  SCM result;
  result = SCM_EOL;
  type = a->type;
  for (i = 0; (i < a->fields_len); i = (1 + i)) {
    field = (type->fields)[(a->fields)[i]];
    result =
      scm_cons((scm_cons((scm_from_uint(((a->fields)[i]))),
                 (scm_from_utf8_stringn((field.name), (field.name_len))))),
        result);
  };
  return (result);
};
SCM scm_from_field_data(void* data, size_t size, db_field_type_t field_type) {
  status_declare;
  SCM b;
  if (!data) {
    return (SCM_BOOL_F);
  };
  if ((db_field_type_binary8 == field_type) ||
    (db_field_type_binary16 == field_type) ||
    (db_field_type_binary32 == field_type) ||
    (db_field_type_binary64 == field_type) ||
    (db_field_type_binary8f == field_type) ||
    (db_field_type_binary16f == field_type) ||
    (db_field_type_binary32f == field_type) ||
    (db_field_type_binary64f == field_type) ||
    (db_field_type_binary128f == field_type) ||
    (db_field_type_binary256f == field_type)) {
    b = scm_c_make_bytevector(size);
    memcpy((SCM_BYTEVECTOR_CONTENTS(b)), data, size);
    return (b);
  } else if ((db_field_type_string8 == field_type) ||
    (db_field_type_string16 == field_type) ||
    (db_field_type_string32 == field_type) ||
    (db_field_type_string64 == field_type) ||
    (db_field_type_string8f == field_type) ||
    (db_field_type_string16f == field_type) ||
    (db_field_type_string32f == field_type) ||
    (db_field_type_string64f == field_type) ||
    (db_field_type_string128f == field_type) ||
    (db_field_type_string256f == field_type)) {
    return ((scm_from_utf8_stringn(data, size)));
  } else if (db_field_type_uint64f == field_type) {
    return ((scm_from_uint64((*((uint64_t*)(data))))));
  } else if (db_field_type_uint32f == field_type) {
    return ((scm_from_uint32((*((uint32_t*)(data))))));
  } else if (db_field_type_uint16f == field_type) {
    return ((scm_from_uint16((*((uint16_t*)(data))))));
  } else if (db_field_type_uint8f == field_type) {
    return ((scm_from_uint8((*((uint8_t*)(data))))));
  } else if (db_field_type_int64f == field_type) {
    return ((scm_from_int64((*((int64_t*)(data))))));
  } else if (db_field_type_int32f == field_type) {
    return ((scm_from_int32((*((int32_t*)(data))))));
  } else if (db_field_type_int16f == field_type) {
    return ((scm_from_int16((*((int16_t*)(data))))));
  } else if (db_field_type_int8f == field_type) {
    return ((scm_from_int8((*((int8_t*)(data))))));
  } else if (db_field_type_float64f == field_type) {
    return ((scm_from_double((*((double*)(data))))));
  } else if ((db_field_type_uint128f == field_type) ||
    (db_field_type_uint256f == field_type) ||
    (db_field_type_int128f == field_type) ||
    (db_field_type_int256f == field_type)) {
    b = scm_c_make_bytevector(size);
    memcpy((SCM_BYTEVECTOR_CONTENTS(b)), data, size);
    return ((scm_first((scm_bytevector_to_uint_list(
      b, scm_endianness_little, (scm_from_size_t(size)))))));
  } else {
    status_set_both_goto(status_group_db_guile, status_id_field_value_invalid);
  };
exit:
  scm_from_status_return(SCM_UNSPECIFIED);
};
status_t scm_to_field_data_integer(SCM scm_a,
  db_field_type_t field_type,
  void** result_data,
  size_t* result_size,
  boolean* result_needs_free) {
  status_declare;
  SCM b;
  size_t size;
  void* data;
  scm_dynwind_begin(0);
  data = 0;
  size = db_field_type_size(field_type);
  if ((db_field_type_uint128f == field_type) ||
    (db_field_type_uint256f == field_type) ||
    (db_field_type_int128f == field_type) ||
    (db_field_type_int256f == field_type)) {
    b = scm_sint_list_to_bytevector(
      (scm_list_1(scm_a)), scm_endianness_little, (scm_from_size_t(size)));
    status_require((db_helper_malloc(size, (&data))));
    scm_dynwind_unwind_handler(free, data, 0);
    memcpy(data, (SCM_BYTEVECTOR_CONTENTS(b)), size);
  } else if ((db_field_type_uint8f == field_type) ||
    (db_field_type_uint16f == field_type) ||
    (db_field_type_uint32f == field_type) ||
    (db_field_type_uint64f == field_type) ||
    (db_field_type_int8f == field_type) ||
    (db_field_type_int16f == field_type) ||
    (db_field_type_int32f == field_type) ||
    (db_field_type_int64f == field_type)) {
    status_require((db_helper_malloc(size, (&data))));
    scm_dynwind_unwind_handler(free, data, 0);
    if (db_field_type_uint8f == field_type) {
      *((uint16_t*)(data)) = scm_to_uint8(scm_a);
    } else if (db_field_type_uint16f == field_type) {
      *((uint16_t*)(data)) = scm_to_uint16(scm_a);
    } else if (db_field_type_uint32f == field_type) {
      *((uint32_t*)(data)) = scm_to_uint32(scm_a);
    } else if (db_field_type_uint64f == field_type) {
      *((uint64_t*)(data)) = scm_to_uint64(scm_a);
    } else if (db_field_type_int8f == field_type) {
      *((int8_t*)(data)) = scm_to_int8(scm_a);
    } else if (db_field_type_int16f == field_type) {
      *((int16_t*)(data)) = scm_to_int16(scm_a);
    } else if (db_field_type_int32f == field_type) {
      *((int32_t*)(data)) = scm_to_int32(scm_a);
    } else if (db_field_type_int64f == field_type) {
      *((int64_t*)(data)) = scm_to_int64(scm_a);
    };
  } else {
    status_set_both_goto(status_group_db_guile, status_id_field_value_invalid);
  };
  *result_data = data;
  *result_size = size;
  *result_needs_free = 1;
exit:
  scm_dynwind_end();
  return (status);
};
status_t scm_to_field_data_string(SCM scm_a,
  db_field_type_t field_type,
  void** result_data,
  size_t* result_size,
  boolean* result_needs_free) {
  status_declare;
  if ((db_field_type_string8 == field_type) ||
    (db_field_type_string16 == field_type) ||
    (db_field_type_string32 == field_type) ||
    (db_field_type_string64 == field_type) ||
    (db_field_type_string8f == field_type) ||
    (db_field_type_string16f == field_type) ||
    (db_field_type_string32f == field_type) ||
    (db_field_type_string64f == field_type) ||
    (db_field_type_string128f == field_type) ||
    (db_field_type_string256f == field_type)) {
    1;
  } else {
    status_set_both_goto(status_group_db_guile, status_id_field_value_invalid);
  };
  *result_needs_free = 1;
  *result_data = scm_to_utf8_stringn(scm_a, 0);
  *result_size = scm_c_string_utf8_length(scm_a);
exit:
  return (status);
};
status_t scm_to_field_data_bytevector(SCM scm_a,
  db_field_type_t field_type,
  void** result_data,
  size_t* result_size,
  boolean* result_needs_free) {
  status_declare;
  if (!(db_field_type_binary8 || db_field_type_binary16 ||
        db_field_type_binary32 || db_field_type_binary64 ||
        db_field_type_binary8f || db_field_type_binary16f ||
        db_field_type_binary32f || db_field_type_binary64f ||
        db_field_type_binary128f || db_field_type_binary256f)) {
    status_set_both_goto(status_group_db_guile, status_id_field_value_invalid);
  };
  *result_needs_free = 0;
  *result_data = SCM_BYTEVECTOR_CONTENTS(scm_a);
  *result_size = SCM_BYTEVECTOR_LENGTH(scm_a);
exit:
  return (status);
};
status_t scm_to_field_data_float(SCM scm_a,
  db_field_type_t field_type,
  void** result_data,
  size_t* result_size,
  boolean* result_needs_free) {
  status_declare;
  size_t size;
  void* data;
  scm_dynwind_begin(0);
  /* there is no scm->float */
  if (db_field_type_float64f == field_type) {
    size = 8;
    status_require((db_helper_malloc(size, (&data))));
    scm_dynwind_unwind_handler(free, data, 0);
    *((double*)(data)) = scm_to_double(scm_a);
  } else {
    status_set_both_goto(status_group_db_guile, status_id_field_value_invalid);
  };
  *result_needs_free = 1;
  *result_data = data;
  *result_size = size;
exit:
  scm_dynwind_end();
  return (status);
};
/** convert an scm value to the format that will be used to for insert.
  result-data has to be freed by the caller only if result-needs-free is true.
  checks if the size of the data fits the field size */
status_t scm_to_field_data(SCM scm_a,
  db_field_type_t field_type,
  void** result_data,
  size_t* result_size,
  boolean* result_needs_free) {
  status_declare;
  if (scm_is_bytevector(scm_a)) {
    return ((scm_to_field_data_bytevector(
      scm_a, field_type, result_data, result_size, result_needs_free)));
  } else if (scm_is_string(scm_a)) {
    return ((scm_to_field_data_string(
      scm_a, field_type, result_data, result_size, result_needs_free)));
  } else if (scm_is_integer(scm_a)) {
    return ((scm_to_field_data_integer(
      scm_a, field_type, result_data, result_size, result_needs_free)));
  } else if (scm_is_rational(scm_a)) {
    return ((scm_to_field_data_float(
      scm_a, field_type, result_data, result_size, result_needs_free)));
  } else {
    status_set_both_goto(status_group_db_guile, status_id_field_value_invalid);
  };
exit:
  return (status);
};
/** this routine allocates result and passes ownership to the caller */
status_t scm_to_db_ids(SCM scm_a, db_ids_t* result) {
  status_declare;
  db_ids_declare(b);
  size_t length;
  length = scm_to_size_t((scm_length(scm_a)));
  scm_dynwind_begin(0);
  status_require((db_ids_new(length, (&b))));
  scm_dynwind_unwind_handler(free, (b.start), 0);
  while (!scm_is_null(scm_a)) {
    db_ids_add(b, (scm_to_uintmax((scm_first(scm_a)))));
    scm_a = scm_tail(scm_a);
  };
  *result = b;
exit:
  scm_dynwind_end();
  return (status);
};
SCM scm_from_db_ids(db_ids_t a) {
  SCM b;
  b = SCM_EOL;
  while (db_ids_in_range(a)) {
    b = scm_cons((scm_from_uint((db_ids_get(a)))), b);
    db_ids_forward(a);
  };
  return (b);
};
SCM scm_from_db_records(db_records_t a) {
  db_record_t* b;
  SCM result;
  result = SCM_EOL;
  while (i_array_in_range(a)) {
    b = scm_gc_malloc((sizeof(db_record_t)), "db-record-t");
    *b = i_array_get(a);
    result = scm_cons((scm_from_db_record(b)), result);
    i_array_forward(a);
  };
  return (result);
};
SCM scm_from_db_relations(db_relations_t a) {
  SCM b;
  db_relation_t record;
  b = SCM_EOL;
  while (db_relations_in_range(a)) {
    record = db_relations_get(a);
    b = scm_cons((scm_vector((scm_list_4((scm_from_uint((record.left))),
                   (scm_from_uint((record.right))),
                   (scm_from_uint((record.label))),
                   (scm_from_uint((record.ordinal))))))),
      b);
    db_relations_forward(a);
  };
  return (b);
};
db_ordinal_t db_guile_ordinal_generator(void* state) {
  SCM scm_state;
  SCM scm_generator;
  SCM scm_result;
  scm_state = *((SCM*)(state));
  scm_generator = scm_first(scm_state);
  scm_result = scm_apply_0(scm_generator, (scm_tail(scm_state)));
  *((SCM*)(state)) = scm_cons(scm_generator, (scm_tail(scm_result)));
  return ((scm_to_uintmax((scm_first(scm_result)))));
};
boolean
db_guile_record_matcher(db_type_t* type, db_record_t record, void* state) {
  SCM scm_state;
  SCM scm_matcher;
  SCM scm_result;
  scm_state = *((SCM*)(state));
  scm_matcher = scm_first(scm_state);
  scm_result = scm_apply_2(scm_matcher,
    (scm_from_db_type(type)),
    (scm_from_db_record((&record))),
    (scm_tail(scm_state)));
  *((SCM*)(state)) = scm_cons(scm_matcher, (scm_tail(scm_result)));
  return ((scm_to_bool((scm_first(scm_result)))));
};
/** "free" compatible memreg-heap-free for use in scm-dynwind-unwind-handler */
void db_guile_memreg_heap_free(void* a) {
  memreg_heap_free((*((memreg_register_t*)(a))));
};
status_t scm_c_to_db_record_values(db_type_t* type,
  SCM scm_values,
  db_record_values_t* result_values,
  memreg_register_t* result_allocations) {
  status_declare;
  db_record_values_declare(values);
  memreg_heap_declare(allocations);
  SCM scm_value;
  db_fields_len_t field_offset;
  boolean field_data_needs_free;
  void* field_data;
  size_t field_data_size;
  if (memreg_heap_allocate(
        (scm_to_size_t((scm_length(scm_values)))), (&allocations))) {
    status_set_both(status_group_db_guile, db_status_id_memory);
    return (status);
  };
  scm_dynwind_begin(0);
  scm_dynwind_unwind_handler(db_guile_memreg_heap_free, (&allocations), 0);
  status_require((db_record_values_new(type, (&values))));
  memreg_heap_add(allocations, (values.data));
  while (!scm_is_null(scm_values)) {
    scm_value = scm_first(scm_values);
    status_require(
      (scm_to_field_offset((scm_first(scm_value)), type, (&field_offset))));
    status_require((scm_to_field_data((scm_tail(scm_value)),
      ((field_offset + type->fields)->type),
      (&field_data),
      (&field_data_size),
      (&field_data_needs_free))));
    if (field_data_needs_free) {
      memreg_heap_add(allocations, field_data);
    };
    db_record_values_set((&values), field_offset, field_data, field_data_size);
    scm_values = scm_tail(scm_values);
  };
  *result_values = values;
exit:
  if (status_is_failure) {
    memreg_heap_free(allocations);
  } else {
    *result_allocations = allocations;
  };
  scm_dynwind_end();
  return (status);
};
#include "./selections.c"
