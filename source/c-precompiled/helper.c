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
#define define_db_relations_to_scm_retrieve(field_name) \
  SCM db_relations_to_scm_retrieve_##field_name(db_relations_t a) { \
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
#define db_env_to_scm(pointer) scm_make_foreign_object_1(scm_type_env, pointer)
#define db_txn_to_scm(pointer) scm_make_foreign_object_1(scm_type_txn, pointer)
#define db_index_to_scm(pointer, env) \
  scm_make_foreign_object_2(scm_type_index, pointer, env)
#define db_type_to_scm(pointer, env) \
  scm_make_foreign_object_2(scm_type_type, pointer, env)
#define db_selection_to_scm(pointer) \
  scm_make_foreign_object_1(scm_type_selection, pointer)
#define scm_to_db_record(a, result) \
  result.id = ((db_id_t)(scm_foreign_object_ref(a, 0))); \
  result.size = ((size_t)(scm_foreign_object_ref(a, 1))); \
  result.data = ((void*)(scm_foreign_object_ref(a, 2)))
#define scm_to_db_env(a) ((db_env_t*)(scm_foreign_object_ref(a, 0)))
#define scm_to_db_txn(a) ((db_txn_t*)(scm_foreign_object_ref(a, 0)))
#define scm_to_db_index(a) ((db_index_t*)(scm_foreign_object_ref(a, 0)))
#define scm_to_db_type(a) ((db_type_t*)(scm_foreign_object_ref(a, 0)))
#define scm_to_db_selection(a, selection_name) \
  ((db_##selection_name##_selection_t*)(scm_foreign_object_ref(a, 0)))
#define scm_type_to_db_env(a) ((db_env_t*)(scm_foreign_object_ref(a, 1)))
#define scm_index_to_db_env(a) ((db_env_t*)(scm_foreign_object_ref(a, 1)))
#define db_status_group_db_guile db_status_group_last
#define status_to_scm_error(a) \
  scm_c_error((db_guile_status_name(a)), (db_guile_status_description(a)))
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
SCM scm_rnrs_raise;
SCM scm_symbol_binary;
SCM scm_symbol_float32;
SCM scm_symbol_float64;
SCM scm_symbol_int16;
SCM scm_symbol_int32;
SCM scm_symbol_int64;
SCM scm_symbol_int8;
SCM scm_symbol_min;
SCM scm_symbol_max;
SCM scm_symbol_label;
SCM scm_symbol_left;
SCM scm_symbol_ordinal;
SCM scm_symbol_right;
SCM scm_symbol_string;
SCM scm_symbol_string16;
SCM scm_symbol_string32;
SCM scm_symbol_string64;
SCM scm_symbol_string8;
SCM scm_symbol_uint16;
SCM scm_symbol_uint32;
SCM scm_symbol_uint64;
SCM scm_symbol_uint8;
SCM scm_type_env;
SCM scm_type_index;
SCM scm_type_record;
SCM scm_type_selection;
SCM scm_type_txn;
SCM scm_type_type;
SCM db_record_to_scm(db_record_t a) {
  SCM b;
  b = scm_make_foreign_object_0(scm_type_record);
  scm_foreign_object_unsigned_set_x(b, 0, (a.id));
  scm_foreign_object_unsigned_set_x(b, 1, (a.size));
  scm_foreign_object_set_x(b, 2, (a.data));
  return (b);
};
/** get the db-field for either a field offset integer or field name */
status_t
scm_to_field_offset(SCM scm_a, db_type_t* type, db_fields_len_t* result) {
  status_declare;
  db_field_t* field;
  uint8_t* field_name;
  field_name = 0;
  if (scm_is_integer(scm_a)) {
    *result = scm_to_uint(scm_a);
  } else {
    field_name = scm_to_utf8_stringn(scm_a, 0);
    field = db_type_field_get(type, field_name);
    free(field_name);
    if (field) {
      *result = field->index;
    } else {
      status_set_both_goto(
        db_status_group_db_guile, status_id_field_name_not_found);
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
  fields_len = scm_to_uint((scm_length(scm_fields)));
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
  if (db_status_group_db_guile == a.group) {
    if (status_id_field_name_not_found == a.id) {
      b = "no field found with given name";
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
  if (db_status_group_db_guile == a.group) {
    if (status_id_field_name_not_found == a.id) {
      b = "field-not-found";
    } else {
      b = "";
    };
  } else {
    b = db_status_name(a);
  };
  return (((uint8_t*)(b)));
};
/** float32 not supported by guile */
db_field_type_t scm_to_db_field_type(SCM a) {
  if (scm_is_eq(scm_symbol_binary, a)) {
    return (1);
  } else if (scm_is_eq(scm_symbol_string, a)) {
    return (3);
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
/** db-index-t* -> SCM:((field-offset . field-name) ...) */
SCM db_index_to_scm_fields(db_index_t* a) {
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
/** convert an scm value to the format that will be used to for insert.
  result-data has to be freed by the caller only if result-is-ref is true */
status_t scm_to_field_data(SCM scm_a,
  db_field_type_t field_type,
  void** result_data,
  size_t* result_size,
  boolean* result_is_ref) {
  status_declare;
  size_t size;
  void* data;
  scm_dynwind_begin(0);
  if (scm_is_bytevector(scm_a)) {
    if (!(db_field_type_binary == field_type)) {
      status_set_id_goto(status_id_field_value_invalid);
    };
    *result_is_ref = 1;
    *result_data = SCM_BYTEVECTOR_CONTENTS(scm_a);
    *result_size = SCM_BYTEVECTOR_LENGTH(scm_a);
  } else if (scm_is_string(scm_a)) {
    size = scm_c_string_utf8_length(scm_a);
    if (db_field_type_string == field_type) {
      1;
    } else if (db_field_type_string64 == field_type) {
      if (8 < size) {
        status_set_id_goto(status_id_field_value_invalid);
      };
    } else if (db_field_type_string32 == field_type) {
      if (4 < size) {
        status_set_id_goto(status_id_field_value_invalid);
      };
    } else if (db_field_type_string16 == field_type) {
      if (2 < size) {
        status_set_id_goto(status_id_field_value_invalid);
      };
    } else if (db_field_type_string8 == field_type) {
      if (1 < size) {
        status_set_id_goto(status_id_field_value_invalid);
      };
    } else {
      status_set_id_goto(status_id_field_value_invalid);
    };
    *result_is_ref = 0;
    *result_data = scm_to_utf8_stringn(scm_a, 0);
    *result_size = size;
  } else if (scm_is_integer(scm_a)) {
    status_require((db_helper_malloc(8, (&data))));
    scm_dynwind_unwind_handler(free, data, 0);
    if (db_field_type_uint64 == field_type) {
      *((uint64_t*)(data)) = scm_to_uint64(scm_a);
      size = 8;
    } else if (db_field_type_uint32 == field_type) {
      *((uint32_t*)(data)) = scm_to_uint32(scm_a);
      size = 4;
    } else if (db_field_type_uint16 == field_type) {
      *((uint16_t*)(data)) = scm_to_uint16(scm_a);
      size = 2;
    } else if (db_field_type_uint8 == field_type) {
      *((uint16_t*)(data)) = scm_to_uint8(scm_a);
      size = 1;
    } else if (db_field_type_int64 == field_type) {
      *((int64_t*)(data)) = scm_to_int64(scm_a);
      size = 8;
    } else if (db_field_type_int32 == field_type) {
      *((int32_t*)(data)) = scm_to_int32(scm_a);
      size = 4;
    } else if (db_field_type_int16 == field_type) {
      *((int16_t*)(data)) = scm_to_int16(scm_a);
      size = 2;
    } else if (db_field_type_int8 == field_type) {
      *((int8_t*)(data)) = scm_to_int8(scm_a);
      size = 1;
    } else {
      status_set_id_goto(status_id_field_value_invalid);
    };
    *result_is_ref = 0;
    *result_data = data;
    *result_size = size;
  } else if (scm_is_rational(scm_a)) {
    status_require((db_helper_malloc(8, (&data))));
    scm_dynwind_unwind_handler(free, data, 0);
    if (db_field_type_float64 == field_type) {
      *((double*)(data)) = scm_to_double(scm_a);
      size = 8;
    } else {
      /* for some reason there is no scm->float */
      status_set_id_goto(status_id_field_value_invalid);
    };
    *result_is_ref = 0;
    *result_data = data;
    *result_size = size;
  } else {
    status_set_id_goto(status_id_field_value_invalid);
  };
exit:
  scm_dynwind_end();
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
    db_ids_add(b, (scm_to_uint((scm_first(scm_a)))));
    scm_a = scm_tail(scm_a);
  };
  *result = b;
exit:
  scm_dynwind_end();
  return (status);
};
SCM db_ids_to_scm(db_ids_t a) {
  SCM b;
  b = SCM_EOL;
  while (db_ids_in_range(a)) {
    b = scm_cons((scm_from_uint((db_ids_get(a)))), b);
    db_ids_forward(a);
  };
  return (b);
};
SCM db_records_to_scm(db_records_t a) {
  db_record_t b;
  SCM c;
  c = SCM_EOL;
  while (i_array_in_range(a)) {
    b = i_array_get(a);
    c = scm_cons((db_record_to_scm(b)), c);
    i_array_forward(a);
  };
  return (c);
};
define_db_relations_to_scm_retrieve(left);
define_db_relations_to_scm_retrieve(right);
define_db_relations_to_scm_retrieve(label);
define_db_relations_to_scm_retrieve(ordinal);
SCM db_relations_to_scm(db_relations_t a) {
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
#include "./selections.c"
