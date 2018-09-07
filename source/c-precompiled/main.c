/* sph-db-guile registers scheme procedures that when called execute specific
 * c-functions that manage calls to sph-db */
#include <libguile.h>
#include <sph-db.h>
#include <sph-db-extra.h>
#include "./foreign/sph/one.c"
#include "./foreign/sph/guile.c"
#include "./helper.c"
SCM scm_db_txn_active_p(SCM a) {
  return ((scm_from_bool((scm_to_db_txn(a)))));
};
SCM scm_db_env_open_p(SCM a) {
  return ((scm_from_bool(((scm_to_db_env(a))->is_open))));
};
SCM scm_db_env_root(SCM a) {
  return ((scm_from_utf8_string(((scm_to_db_env(a))->root))));
};
SCM scm_db_env_maxkeysize(SCM a) {
  return ((scm_from_uint32(((scm_to_db_env(a))->maxkeysize))));
};
SCM scm_db_env_format(SCM a) {
  return ((scm_from_uint32(((scm_to_db_env(a))->format))));
};
SCM scm_db_type_id(SCM a) {
  return ((scm_from_uint(((scm_to_db_type(a))->id))));
};
SCM scm_db_type_name(SCM a) {
  return ((scm_from_utf8_string(((scm_to_db_type(a))->name))));
};
SCM scm_db_type_flags(SCM a) {
  return ((scm_from_uint8(((scm_to_db_type(a))->flags))));
};
SCM scm_db_type_virtual_p(SCM a) {
  return ((scm_from_bool((db_type_is_virtual((scm_to_db_type(a)))))));
};
SCM scm_db_type_fields(SCM a) {
  db_fields_len_t i;
  db_type_t* type;
  db_fields_len_t fields_len;
  db_field_t field;
  db_field_t* fields;
  SCM result;
  result = SCM_EOL;
  type = scm_to_db_type(a);
  fields = type->fields;
  fields_len = type->fields_len;
  i = fields_len;
  while (i) {
    i = (i - 1);
    field = fields[i];
    result = scm_cons(
      (scm_cons((scm_from_utf8_stringn((field.name), (field.name_len))),
        (db_field_type_to_scm((field.type))))),
      result);
  };
  return (result);
};
SCM scm_db_type_indices(SCM scm_type) {
  db_indices_len_t i;
  db_index_t* indices;
  db_indices_len_t indices_len;
  SCM result;
  db_type_t* type;
  result = SCM_EOL;
  type = scm_to_db_type(scm_type);
  indices_len = type->indices_len;
  indices = type->indices;
  for (i = 0; (i < indices_len); i = (1 + i)) {
    result = scm_cons((db_index_to_scm_fields((i + indices))), result);
  };
  return (result);
};
SCM scm_db_open(SCM scm_root, SCM scm_options) {
  status_declare;
  db_env_declare(env);
  db_open_options_t options;
  db_open_options_t* options_pointer;
  SCM a;
  uint8_t* root;
  root = 0;
  root = scm_to_utf8_stringn(scm_root, 0);
  status_require((db_env_new((&env))));
  if (scm_is_undefined(scm_options) || scm_is_null(scm_options)) {
    options_pointer = 0;
  } else {
    db_open_options_set_defaults((&options));
    scm_options_get(scm_options, "is-read-only", a);
    if (scm_is_bool(a)) {
      options.is_read_only = scm_is_true(a);
    };
    scm_options_get(scm_options, "maximum-reader-count", a);
    if (scm_is_integer(a)) {
      options.maximum_reader_count = scm_to_uint(a);
    };
    scm_options_get(scm_options, "filesystem-has-ordered-writes", a);
    if (scm_is_bool(a)) {
      options.filesystem_has_ordered_writes = scm_is_true(a);
    };
    scm_options_get(scm_options, "env-open-flags", a);
    if (scm_is_integer(a)) {
      options.env_open_flags = scm_to_uint(a);
    };
    scm_options_get(scm_options, "file-permissions", a);
    if (scm_is_integer(a)) {
      options.file_permissions = scm_to_uint(a);
    };
    options_pointer = &options;
  };
  status = db_open(root, options_pointer, env);
exit:
  free(root);
  if (status_is_success) {
    return ((db_env_to_scm(env)));
  } else {
    db_close(env);
    free(env);
    status_to_scm_error(status);
  };
};
SCM scm_db_close(SCM scm_env) {
  db_env_declare(env);
  env = scm_to_db_env(scm_env);
  scm_gc();
  db_close(env);
  free(env);
  return (SCM_UNSPECIFIED);
};
SCM scm_db_statistics(SCM scm_txn) {
  status_declare;
  SCM b;
  db_statistics_t a;
  status_require((db_statistics((*(scm_to_db_txn(scm_txn))), (&a))));
  b = SCM_EOL;
  b = scm_acons(
    (scm_from_latin1_symbol("system")), (scm_from_mdb_stat((a.system))), b);
  b = scm_acons(
    (scm_from_latin1_symbol("records")), (scm_from_mdb_stat((a.records))), b);
  b = scm_acons((scm_from_latin1_symbol("relation-lr")),
    (scm_from_mdb_stat((a.relation_lr))),
    b);
  b = scm_acons((scm_from_latin1_symbol("relation-rl")),
    (scm_from_mdb_stat((a.relation_rl))),
    b);
  b = scm_acons((scm_from_latin1_symbol("relation-ll")),
    (scm_from_mdb_stat((a.relation_ll))),
    b);
exit:
  status_to_scm_return(b);
};
SCM scm_db_txn_abort(SCM scm_txn) {
  db_guile_selections_free();
  db_txn_t* txn;
  txn = scm_to_db_txn(scm_txn);
  db_txn_abort(txn);
  free(txn);
  scm_foreign_object_set_x(scm_txn, 0, 0);
  return (SCM_UNSPECIFIED);
};
/** note that commit frees cursors. db-guile-selections-free closes cursors.
  if db-guile-selections-free is called after db-txn-commit a double free occurs
*/
SCM scm_db_txn_commit(SCM scm_txn) {
  status_declare;
  db_guile_selections_free();
  db_txn_t* txn;
  txn = scm_to_db_txn(scm_txn);
  status_require((db_txn_commit(txn)));
  free(txn);
  scm_foreign_object_set_x(scm_txn, 0, 0);
exit:
  status_to_scm_return(SCM_UNSPECIFIED);
};
SCM scm_db_txn_begin(SCM scm_env) {
  status_declare;
  db_txn_t* txn;
  txn = 0;
  status_require((db_helper_calloc((sizeof(db_txn_t)), (&txn))));
  txn->env = scm_to_db_env(scm_env);
  status_require((db_txn_begin(txn)));
exit:
  if (status_is_success) {
    return ((db_txn_to_scm(txn)));
  } else {
    free(txn);
    status_to_scm_error(status);
    return (SCM_UNSPECIFIED);
  };
};
SCM scm_db_txn_write_begin(SCM scm_env) {
  status_declare;
  db_txn_t* txn;
  txn = 0;
  status_require((db_helper_calloc((sizeof(db_txn_t)), (&txn))));
  txn->env = scm_to_db_env(scm_env);
  status_require((db_txn_write_begin(txn)));
exit:
  if (status_is_success) {
    return ((db_txn_to_scm(txn)));
  } else {
    free(txn);
    status_to_scm_error(status);
    return (SCM_UNSPECIFIED);
  };
};
SCM scm_db_status_description(SCM id_status, SCM id_group) {
  status_declare;
  status_set_both((scm_to_int(id_group)), (scm_to_int(id_status)));
  scm_from_latin1_string((db_status_description(status)));
};
SCM scm_db_status_group_id_to_name(SCM a) {
  scm_from_latin1_symbol((db_status_group_id_to_name((scm_to_int(a)))));
};
SCM scm_db_type_create(SCM scm_env,
  SCM scm_name,
  SCM scm_fields,
  SCM scm_flags) {
  status_declare;
  uint8_t* field_name;
  db_name_len_t field_name_len;
  db_field_type_t field_type;
  db_field_t* fields;
  db_fields_len_t fields_len;
  uint8_t flags;
  db_fields_len_t i;
  uint8_t* name;
  SCM scm_field;
  db_type_t* type;
  name = scm_to_utf8_stringn(scm_name, 0);
  scm_dynwind_begin(0);
  scm_dynwind_free(name);
  flags = (scm_is_undefined(scm_flags) ? 0 : scm_to_uint8(scm_flags));
  fields_len = scm_to_uint((scm_length(scm_fields)));
  status_require(
    (db_helper_calloc((fields_len * sizeof(db_field_t)), (&fields))));
  scm_dynwind_free(fields);
  for (i = 0; (i < fields_len);
       i = (1 + i), scm_fields = scm_tail(scm_fields)) {
    scm_field = scm_first(scm_fields);
    field_name = scm_to_utf8_stringn((scm_first(scm_field)), 0);
    scm_dynwind_free(field_name);
    field_name_len = strlen(field_name);
    field_type = scm_to_db_field_type((scm_tail(scm_field)));
    db_field_set((fields[i]), field_type, field_name, field_name_len);
  };
  status_require((db_type_create(
    (scm_to_db_env(scm_env)), name, fields, fields_len, flags, (&type))));
exit:
  scm_dynwind_end();
  status_to_scm_return((db_type_to_scm(type, (scm_to_db_env(scm_env)))));
};
SCM scm_db_type_get(SCM scm_env, SCM scm_name_or_id) {
  db_type_t* type;
  uint8_t* name;
  scm_dynwind_begin(0);
  if (scm_is_string(scm_name_or_id)) {
    name = scm_to_utf8_stringn(scm_name_or_id, 0);
    scm_dynwind_free(name);
    type = db_type_get((scm_to_db_env(scm_env)), name);
  } else {
    type = db_type_get_by_id(
      (scm_to_db_env(scm_env)), (scm_to_uint(scm_name_or_id)));
  };
  scm_dynwind_end();
  return ((type ? db_type_to_scm(type, (scm_to_db_env(scm_env))) : SCM_BOOL_F));
};
SCM scm_db_type_delete(SCM scm_type) {
  status_declare;
  status_require((db_type_delete(
    (scm_type_to_db_env(scm_type)), ((scm_to_db_type(scm_type))->id))));
exit:
  status_to_scm_return(SCM_UNSPECIFIED);
};
SCM scm_db_index_create(SCM scm_type, SCM scm_fields) {
  status_declare;
  db_fields_len_t* fields;
  db_fields_len_t fields_len;
  db_index_t* index;
  status_require(
    (scm_to_field_offsets(scm_type, scm_fields, (&fields), (&fields_len))));
  status_require((db_index_create((scm_type_to_db_env(scm_type)),
    (scm_to_db_type(scm_type)),
    fields,
    fields_len,
    (&index))));
exit:
  status_to_scm_return(
    (db_index_to_scm(index, (scm_type_to_db_env(scm_type)))));
};
SCM scm_db_index_get(SCM scm_type, SCM scm_fields) {
  status_declare;
  db_fields_len_t* fields;
  db_fields_len_t fields_len;
  db_index_t* index;
  status_require(
    (scm_to_field_offsets(scm_type, scm_fields, (&fields), (&fields_len))));
  index = db_index_get((scm_to_db_type(scm_type)), fields, fields_len);
exit:
  status_to_scm_return(
    (index ? db_index_to_scm(index, (scm_type_to_db_env(scm_type)))
           : SCM_BOOL_F));
};
SCM scm_db_index_delete(SCM scm_index) {
  status_declare;
  status_require((db_index_delete(
    (scm_index_to_db_env(scm_index)), (scm_to_db_index(scm_index)))));
exit:
  status_to_scm_return(SCM_UNSPECIFIED);
};
SCM scm_db_index_rebuild(SCM scm_index) {
  status_declare;
  status_require((db_index_rebuild(
    (scm_index_to_db_env(scm_index)), (scm_to_db_index(scm_index)))));
exit:
  status_to_scm_return(SCM_UNSPECIFIED);
};
SCM scm_db_index_fields(SCM scm_index) {
  return ((db_index_to_scm_fields((scm_to_db_index(scm_index)))));
};
SCM scm_db_record_create(SCM scm_txn, SCM scm_type, SCM scm_values) {
  status_declare;
  db_record_values_declare(values);
  void* field_data;
  size_t field_data_size;
  boolean field_data_is_ref;
  SCM scm_value;
  db_fields_len_t field_offset;
  db_fields_len_t i;
  db_id_t result_id;
  db_type_t* type;
  type = scm_to_db_type(scm_type);
  scm_dynwind_begin(0);
  status_require((db_record_values_new(type, (&values))));
  scm_dynwind_unwind_handler(((void (*)(void*))(db_record_values_free)),
    (&values),
    SCM_F_WIND_EXPLICITLY);
  /* set field values */
  while (!scm_is_null(scm_values)) {
    scm_value = scm_first(scm_values);
    status_require(
      (scm_to_field_offset((scm_first(scm_value)), type, (&field_offset))));
    status_require((scm_to_field_data((scm_tail(scm_value)),
      ((field_offset + type->fields)->type),
      (&field_data),
      (&field_data_size),
      (&field_data_is_ref))));
    if (!field_data_is_ref) {
      scm_dynwind_unwind_handler(free, field_data, SCM_F_WIND_EXPLICITLY);
    };
    db_record_values_set((&values), field_offset, field_data, field_data_size);
    scm_values = scm_tail(scm_values);
  };
  /* save */
  status_require(
    (db_record_create((*(scm_to_db_txn(scm_txn))), values, (&result_id))));
exit:
  scm_dynwind_end();
  status_to_scm_return((scm_from_uint(result_id)));
};
db_ordinal_t db_guile_ordinal_generator(void* state) {
  SCM scm_state;
  SCM scm_generator;
  SCM scm_result;
  scm_state = *((SCM*)(state));
  scm_generator = scm_first(scm_state);
  scm_result = scm_apply_0(scm_generator, (scm_tail(scm_state)));
  *((SCM*)(state)) = scm_cons(scm_generator, scm_result);
  return ((scm_to_uint((scm_first(scm_result)))));
};
SCM scm_db_relation_ensure(SCM scm_txn,
  SCM scm_left,
  SCM scm_right,
  SCM scm_label,
  SCM scm_ordinal_generator,
  SCM scm_ordinal_state) {
  status_declare;
  db_ids_t left;
  db_ids_t right;
  db_ids_t label;
  SCM scm_state;
  db_relation_ordinal_generator_t ordinal_generator;
  void* ordinal_state;
  db_ordinal_t ordinal_value;
  status_require((scm_to_db_ids(scm_left, (&left))));
  status_require((scm_to_db_ids(scm_right, (&right))));
  status_require((scm_to_db_ids(scm_label, (&label))));
  if (scm_is_true((scm_procedure_p(scm_ordinal_generator)))) {
    scm_state = scm_cons(scm_ordinal_generator,
      (scm_is_true((scm_list_p(scm_ordinal_state)))
          ? scm_ordinal_state
          : scm_list_1(scm_ordinal_state)));
    ordinal_state = &scm_state;
    ordinal_generator = db_guile_ordinal_generator;
  } else {
    ordinal_generator = 0;
    ordinal_value =
      (scm_is_undefined(scm_ordinal_state) ? 0
                                           : scm_to_uint(scm_ordinal_state));
    ordinal_state = &ordinal_value;
  };
  db_relation_ensure((*(scm_to_db_txn(scm_txn))),
    left,
    right,
    label,
    ordinal_generator,
    ordinal_state);
exit:
  status_to_scm_return(SCM_BOOL_T);
};
SCM scm_db_id_type(SCM a) { scm_from_uint((db_id_type((scm_to_uint(a))))); };
SCM scm_db_id_element(SCM a) {
  scm_from_uint((db_id_element((scm_to_uint(a)))));
};
SCM scm_db_id_add_type(SCM a, SCM type) {
  scm_from_uint((db_id_add_type((scm_to_uint(a)), (scm_to_uint(type)))));
};
SCM scm_db_record_get(SCM scm_txn, SCM scm_ids) {
  status_declare;
  db_ids_t ids;
  db_records_t records;
  SCM result;
  scm_dynwind_begin(0);
  status_require((scm_to_db_ids(scm_ids, (&ids))));
  scm_dynwind_free((ids.start));
  status_require((db_records_new((db_ids_length(ids)), (&records))));
  scm_dynwind_free((records.start));
  status_require((db_record_get((*(scm_to_db_txn(scm_txn))), ids, (&records))));
  result = db_records_to_scm(records);
exit:
  scm_dynwind_end();
  status_to_scm_return(result);
};
SCM scm_db_relation_select(SCM scm_txn,
  SCM scm_left,
  SCM scm_right,
  SCM scm_label,
  SCM scm_retrieve,
  SCM scm_ordinal,
  SCM scm_offset) {
  status_declare;
  db_ids_t left;
  db_ids_t right;
  SCM scm_ordinal_min;
  SCM (*relations_to_scm)(db_relations_t);
  SCM scm_ordinal_max;
  db_ids_t label;
  db_ids_t* left_pointer;
  db_ids_t* right_pointer;
  db_ids_t* label_pointer;
  db_ordinal_condition_t ordinal;
  db_ordinal_condition_t* ordinal_pointer;
  uint32_t offset;
  db_guile_relation_selection_t* selection;
  if (scm_is_null(scm_left) || scm_is_null(scm_right) ||
    scm_is_null(scm_label)) {
    return ((db_selection_to_scm(0)));
  };
  /* dont start the dynwind context sooner or it might not be finished */
  scm_dynwind_begin(0);
  /* left/right/label */
  if (scm_is_pair(scm_left)) {
    status_require((scm_to_db_ids(scm_left, (&left))));
    left_pointer = &left;
  } else {
    left_pointer = 0;
  };
  if (scm_is_pair(scm_right)) {
    status_require((scm_to_db_ids(scm_right, (&right))));
    right_pointer = &right;
  } else {
    right_pointer = 0;
  };
  if (scm_is_pair(scm_right)) {
    status_require((scm_to_db_ids(scm_label, (&label))));
    label_pointer = &label;
  } else {
    label_pointer = 0;
  };
  /* ordinal */
  if (scm_is_true((scm_list_p(scm_ordinal)))) {
    scm_ordinal_min = scm_assoc_ref(scm_ordinal, scm_symbol_min);
    scm_ordinal_max = scm_assoc_ref(scm_ordinal, scm_symbol_max);
    ordinal.min =
      (scm_is_integer(scm_ordinal_min) ? scm_to_uint(scm_ordinal_min) : 0);
    ordinal.max =
      (scm_is_integer(scm_ordinal_max) ? scm_to_uint(scm_ordinal_max) : 0);
    ordinal_pointer = &ordinal;
  } else {
    ordinal_pointer = 0;
  };
  /* offset */
  offset = (scm_is_integer(scm_offset) ? scm_to_uint32(scm_offset) : 0);
  /* retrieve */
  relations_to_scm = (scm_is_symbol(scm_retrieve)
      ? (scm_is_eq(scm_symbol_right, scm_retrieve)
            ? db_relations_to_scm_retrieve_right
            : (scm_is_eq(scm_symbol_left, scm_retrieve)
                  ? db_relations_to_scm_retrieve_left
                  : (scm_is_eq(scm_symbol_label, scm_retrieve)
                        ? db_relations_to_scm_retrieve_label
                        : (scm_is_eq(scm_symbol_ordinal, scm_retrieve)
                              ? db_relations_to_scm_retrieve_ordinal
                              : 0))))
      : db_relations_to_scm);
  if (!relations_to_scm) {
    status_set_both_goto(db_status_group_db_guile, status_id_invalid_argument);
  };
  /* db-relation-select */
  status_require(
    (db_helper_malloc((sizeof(db_guile_relation_selection_t)), (&selection))));
  scm_dynwind_unwind_handler(free, selection, 0);
  status_require((db_relation_select((*(scm_to_db_txn(scm_txn))),
    left_pointer,
    right_pointer,
    label_pointer,
    ordinal_pointer,
    offset,
    (&(selection->selection)))));
  selection->left = left;
  selection->right = right;
  selection->label = label;
  selection->relations_to_scm = relations_to_scm;
exit:
  scm_dynwind_end();
  status_to_scm_return((db_selection_to_scm(selection)));
};
SCM scm_db_relation_read(SCM scm_selection, SCM scm_count);
SCM scm_db_record_select(SCM scm_txn);
SCM scm_db_record_read(SCM scm_txn);
SCM scm_db_record_ref(SCM scm_type, SCM scm_record, SCM scm_field);
SCM scm_db_index_select(SCM scm_txn);
SCM scm_db_index_read(SCM scm_txn);
SCM scm_db_record_index_select(SCM scm_txn);
SCM scm_db_record_index_read(SCM scm_txn);
SCM scm_db_record_virtual(SCM scm_data);
/** prepare scm valuaes and register guile bindings */
void db_guile_init() {
  SCM type_slots;
  SCM scm_symbol_data;
  scm_rnrs_raise = scm_c_public_ref("rnrs exceptions", "raise");
  scm_symbol_min = scm_from_latin1_symbol("min");
  scm_symbol_max = scm_from_latin1_symbol("max");
  scm_symbol_binary = scm_from_latin1_symbol("binary");
  scm_symbol_data = scm_from_latin1_symbol("data");
  scm_symbol_float32 = scm_from_latin1_symbol("float32");
  scm_symbol_float64 = scm_from_latin1_symbol("float64");
  scm_symbol_int16 = scm_from_latin1_symbol("int16");
  scm_symbol_int32 = scm_from_latin1_symbol("int32");
  scm_symbol_int64 = scm_from_latin1_symbol("int64");
  scm_symbol_int8 = scm_from_latin1_symbol("int8");
  scm_symbol_label = scm_from_latin1_symbol("label");
  scm_symbol_left = scm_from_latin1_symbol("left");
  scm_symbol_ordinal = scm_from_latin1_symbol("ordinal");
  scm_symbol_right = scm_from_latin1_symbol("right");
  scm_symbol_string = scm_from_latin1_symbol("string");
  scm_symbol_string16 = scm_from_latin1_symbol("string16");
  scm_symbol_string32 = scm_from_latin1_symbol("string32");
  scm_symbol_string64 = scm_from_latin1_symbol("string64");
  scm_symbol_string8 = scm_from_latin1_symbol("string8");
  scm_symbol_uint16 = scm_from_latin1_symbol("uint16");
  scm_symbol_uint32 = scm_from_latin1_symbol("uint32");
  scm_symbol_uint64 = scm_from_latin1_symbol("uint64");
  scm_symbol_uint8 = scm_from_latin1_symbol("uint8");
  type_slots = scm_list_1(scm_symbol_data);
  scm_type_env = scm_make_foreign_object_type(
    (scm_from_latin1_symbol("db-env")), type_slots, 0);
  scm_type_txn = scm_make_foreign_object_type(
    (scm_from_latin1_symbol("db-txn")), type_slots, 0);
  type_slots = scm_list_2(scm_symbol_data, (scm_from_latin1_symbol("env")));
  scm_type_type = scm_make_foreign_object_type(
    (scm_from_latin1_symbol("db-type")), type_slots, 0);
  scm_type_index = scm_make_foreign_object_type(
    (scm_from_latin1_symbol("db-index")), type_slots, 0);
  type_slots = scm_list_3((scm_from_latin1_symbol("id")),
    (scm_from_latin1_symbol("size")),
    scm_symbol_data);
  scm_type_record = scm_make_foreign_object_type(
    (scm_from_latin1_symbol("db-record")), type_slots, 0);
  scm_type_selection = scm_make_foreign_object_type(
    (scm_from_latin1_symbol("db-selection")), type_slots, 0);
  scm_c_define_procedure_c_init;
  SCM m = scm_c_resolve_module("sph db");
  scm_c_module_define(
    m, "db-type-flag-virtual", (scm_from_uint(db_type_flag_virtual)));
  scm_c_define_procedure_c("db-open",
    1,
    1,
    0,
    scm_db_open,
    ("string:root [((key . value) ...):options] ->"));
  scm_c_define_procedure_c(
    "db-close", 1, 0, 0, scm_db_close, "deinitialises the database handle");
  scm_c_define_procedure_c("db-env-open?", 1, 0, 0, scm_db_env_open_p, "");
  scm_c_define_procedure_c(
    "db-env-maxkeysize", 1, 0, 0, scm_db_env_maxkeysize, "");
  scm_c_define_procedure_c("db-env-root", 1, 0, 0, scm_db_env_root, "");
  scm_c_define_procedure_c("db-env-format", 1, 0, 0, scm_db_env_format, "");
  scm_c_define_procedure_c("db-statistics", 1, 0, 0, scm_db_statistics, "");
  scm_c_define_procedure_c(
    "db-txn-begin", 1, 0, 0, scm_db_txn_begin, ("-> db-txn"));
  scm_c_define_procedure_c(
    "db-txn-write-begin", 1, 0, 0, scm_db_txn_write_begin, ("-> db-txn"));
  scm_c_define_procedure_c(
    "db-txn-abort", 1, 0, 0, scm_db_txn_abort, ("db-txn -> unspecified"));
  scm_c_define_procedure_c(
    "db-txn-commit", 1, 0, 0, scm_db_txn_commit, ("db-txn -> unspecified"));
  scm_c_define_procedure_c(
    "db-txn-active?", 1, 0, 0, scm_db_txn_active_p, ("db-txn -> boolean"));
  scm_c_define_procedure_c("db-status-description",
    2,
    0,
    0,
    scm_db_status_description,
    ("integer:id-status integer:id-group -> string"));
  scm_c_define_procedure_c(("db-status-group-id->name"),
    1,
    0,
    0,
    scm_db_status_group_id_to_name,
    ("integer -> symbol"));
  scm_c_define_procedure_c("db-type-create", 3, 1, 0, scm_db_type_create, "");
  scm_c_define_procedure_c("db-type-delete", 1, 0, 0, scm_db_type_delete, "");
  scm_c_define_procedure_c("db-type-get", 2, 0, 0, scm_db_type_get, "");
  scm_c_define_procedure_c("db-type-id", 1, 0, 0, scm_db_type_id, "");
  scm_c_define_procedure_c("db-type-name", 1, 0, 0, scm_db_type_name, "");
  scm_c_define_procedure_c("db-type-indices", 1, 0, 0, scm_db_type_indices, "");
  scm_c_define_procedure_c("db-type-fields", 1, 0, 0, scm_db_type_fields, "");
  scm_c_define_procedure_c(
    "db-type-virtual?", 1, 0, 0, scm_db_type_virtual_p, "");
  scm_c_define_procedure_c("db-type-flags", 1, 0, 0, scm_db_type_flags, "");
  scm_c_define_procedure_c("db-index-create", 2, 0, 0, scm_db_index_create, "");
  scm_c_define_procedure_c("db-index-delete", 1, 0, 0, scm_db_index_delete, "");
  scm_c_define_procedure_c("db-index-get", 2, 0, 0, scm_db_index_get, "");
  scm_c_define_procedure_c(
    "db-index-rebuild", 1, 0, 0, scm_db_index_rebuild, "");
  scm_c_define_procedure_c("db-index-fields", 1, 0, 0, scm_db_index_fields, "");
  scm_c_define_procedure_c("db-id-type", 1, 0, 0, scm_db_id_type, "");
  scm_c_define_procedure_c("db-id-element", 1, 0, 0, scm_db_id_element, "");
  scm_c_define_procedure_c("db-id-add-type", 2, 0, 0, scm_db_id_add_type, "");
  scm_c_define_procedure_c("db-record-create",
    3,
    0,
    0,
    scm_db_record_create,
    ("db-txn db-type list:((field-id . value) ...) -> integer"));
  scm_c_define_procedure_c("db-relation-ensure",
    4,
    2,
    0,
    scm_db_relation_ensure,
    ("db-txn list:left list:right list:label [ordinal-generator "
     "ordinal-state]"));
};