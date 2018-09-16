/* sph-db-guile registers scheme procedures that when called execute specific
 * c-functions that manage calls to sph-db */
#include <libguile.h>
#include <sph-db.h>
#include <sph-db-extra.h>
#include "./foreign/sph/one.c"
#include "./foreign/sph/guile.c"
#include "./foreign/sph/memreg.c"
#include "./foreign/sph/memreg-heap.c"
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
  return ((scm_from_uintmax(((scm_to_db_type(a))->id))));
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
        (scm_from_db_field_type((field.type))))),
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
    result = scm_cons((scm_from_db_index_fields((i + indices))), result);
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
      options.maximum_reader_count = scm_to_uint32(a);
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
    return ((scm_from_db_env(env)));
  } else {
    db_close(env);
    free(env);
    scm_from_status_error(status);
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
  scm_from_status_return(b);
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
  scm_from_status_return(SCM_UNSPECIFIED);
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
    return ((scm_from_db_txn(txn)));
  } else {
    free(txn);
    scm_from_status_error(status);
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
    return ((scm_from_db_txn(txn)));
  } else {
    free(txn);
    scm_from_status_error(status);
    return (SCM_UNSPECIFIED);
  };
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
  fields_len = scm_to_uintmax((scm_length(scm_fields)));
  status_require(
    (db_helper_calloc((fields_len * sizeof(db_field_t)), (&fields))));
  scm_dynwind_free(fields);
  for (i = 0; (i < fields_len);
       i = (1 + i), scm_fields = scm_tail(scm_fields)) {
    scm_field = scm_first(scm_fields);
    if (scm_is_symbol(scm_field)) {
      field_type = scm_to_db_field_type(scm_field);
      field_name = "";
    } else {
      /* pair */
      field_name = scm_to_utf8_stringn((scm_first(scm_field)), 0);
      field_type = scm_to_db_field_type((scm_tail(scm_field)));
      field_name_len = strlen(field_name);
      scm_dynwind_free(field_name);
    };
    db_field_set((fields[i]), field_type, field_name, field_name_len);
  };
  status_require((db_type_create(
    (scm_to_db_env(scm_env)), name, fields, fields_len, flags, (&type))));
exit:
  scm_dynwind_end();
  scm_from_status_return((scm_from_db_type(type)));
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
      (scm_to_db_env(scm_env)), (scm_to_uintmax(scm_name_or_id)));
  };
  scm_dynwind_end();
  return ((type ? scm_from_db_type(type) : SCM_BOOL_F));
};
SCM scm_db_type_delete(SCM scm_env, SCM scm_type) {
  status_declare;
  status_require((db_type_delete(
    (scm_to_db_env(scm_env)), ((scm_to_db_type(scm_type))->id))));
exit:
  scm_from_status_return(SCM_UNSPECIFIED);
};
SCM scm_db_index_create(SCM scm_env, SCM scm_type, SCM scm_fields) {
  status_declare;
  db_fields_len_t* fields;
  db_fields_len_t fields_len;
  db_index_t* index;
  status_require(
    (scm_to_field_offsets(scm_type, scm_fields, (&fields), (&fields_len))));
  status_require((db_index_create((scm_to_db_env(scm_env)),
    (scm_to_db_type(scm_type)),
    fields,
    fields_len,
    (&index))));
exit:
  scm_from_status_return((scm_from_db_index(index)));
};
SCM scm_db_index_get(SCM scm_env, SCM scm_type, SCM scm_fields) {
  status_declare;
  db_fields_len_t* fields;
  db_fields_len_t fields_len;
  db_index_t* index;
  status_require(
    (scm_to_field_offsets(scm_type, scm_fields, (&fields), (&fields_len))));
  index = db_index_get((scm_to_db_type(scm_type)), fields, fields_len);
exit:
  scm_from_status_return((index ? scm_from_db_index(index) : SCM_BOOL_F));
};
SCM scm_db_index_delete(SCM scm_env, SCM scm_index) {
  status_declare;
  status_require(
    (db_index_delete((scm_to_db_env(scm_env)), (scm_to_db_index(scm_index)))));
exit:
  scm_from_status_return(SCM_UNSPECIFIED);
};
SCM scm_db_index_rebuild(SCM scm_env, SCM scm_index) {
  status_declare;
  status_require(
    (db_index_rebuild((scm_to_db_env(scm_env)), (scm_to_db_index(scm_index)))));
exit:
  scm_from_status_return(SCM_UNSPECIFIED);
};
SCM scm_db_index_fields(SCM scm_index) {
  return ((scm_from_db_index_fields((scm_to_db_index(scm_index)))));
};
SCM scm_db_record_create(SCM scm_txn, SCM scm_type, SCM scm_values) {
  status_declare;
  db_record_values_declare(values);
  memreg_heap_declare(allocations);
  SCM scm_value;
  db_id_t result_id;
  db_type_t* type;
  scm_dynwind_begin(0);
  type = scm_to_db_type(scm_type);
  debug_log("%d", 4);
  status_require(
    (scm_c_to_db_record_values(type, scm_values, (&values), (&allocations))));
  debug_log("%d", 5);
  scm_dynwind_unwind_handler(
    db_guile_memreg_heap_free, (&allocations), SCM_F_WIND_EXPLICITLY);
exit:
  debug_log("%d", 6);
  scm_dynwind_end();
  debug_log("%d", 7);
  scm_from_status_return((scm_from_uintmax(result_id)));
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
  /* ordinal setter. generated by procedure or fixed value */
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
                                           : scm_to_uintmax(scm_ordinal_state));
    ordinal_state = &ordinal_value;
  };
  db_relation_ensure((*(scm_to_db_txn(scm_txn))),
    left,
    right,
    label,
    ordinal_generator,
    ordinal_state);
exit:
  scm_from_status_return(SCM_BOOL_T);
};
SCM scm_db_id_type(SCM a) {
  scm_from_uintmax((db_id_type((scm_to_uintmax(a)))));
};
SCM scm_db_id_element(SCM a) {
  scm_from_uintmax((db_id_element((scm_to_uintmax(a)))));
};
SCM scm_db_id_add_type(SCM a, SCM type) {
  scm_from_uintmax(
    (db_id_add_type((scm_to_uintmax(a)), (scm_to_uintmax(type)))));
};
SCM scm_db_record_get(SCM scm_txn, SCM scm_ids) {
  status_declare;
  db_ids_t ids;
  db_records_t records;
  SCM result;
  db_txn_t txn;
  txn = *(scm_to_db_txn(scm_txn));
  scm_dynwind_begin(0);
  status_require((scm_to_db_ids(scm_ids, (&ids))));
  scm_dynwind_free((ids.start));
  status_require((db_records_new((db_ids_length(ids)), (&records))));
  scm_dynwind_free((records.start));
  status_require((db_record_get(txn, ids, (&records))));
  result = scm_from_db_records(records);
exit:
  scm_dynwind_end();
  scm_from_status_return(result);
};
SCM scm_db_relation_select(SCM scm_txn,
  SCM scm_left,
  SCM scm_right,
  SCM scm_label,
  SCM scm_retrieve,
  SCM scm_ordinal) {
  status_declare;
  db_ids_t label;
  db_ids_t* label_pointer;
  db_ids_t left;
  db_ids_t* left_pointer;
  uint32_t offset;
  db_ordinal_condition_t ordinal;
  db_ordinal_condition_t* ordinal_pointer;
  db_ids_t right;
  db_ids_t* right_pointer;
  SCM (*scm_from_relations)(db_relations_t);
  SCM scm_ordinal_max;
  SCM scm_ordinal_min;
  SCM scm_selection;
  db_guile_relation_selection_t* selection;
  memreg_init(5);
  if (scm_is_null(scm_left) || scm_is_null(scm_right) ||
    scm_is_null(scm_label)) {
    return ((scm_from_db_selection(0)));
  };
  selection = 0;
  scm_dynwind_begin(0);
  /* left/right/label */
  if (scm_is_pair(scm_left)) {
    status_require((scm_to_db_ids(scm_left, (&left))));
    scm_dynwind_unwind_handler(free, (left.start), 0);
    memreg_add((left.start));
    left_pointer = &left;
  } else {
    left_pointer = 0;
  };
  if (scm_is_pair(scm_right)) {
    status_require((scm_to_db_ids(scm_right, (&right))));
    scm_dynwind_unwind_handler(free, (right.start), 0);
    memreg_add((right.start));
    right_pointer = &right;
  } else {
    right_pointer = 0;
  };
  if (scm_is_pair(scm_right)) {
    status_require((scm_to_db_ids(scm_label, (&label))));
    scm_dynwind_unwind_handler(free, (label.start), 0);
    memreg_add((label.start));
    label_pointer = &label;
  } else {
    label_pointer = 0;
  };
  /* ordinal */
  if (scm_is_true((scm_list_p(scm_ordinal)))) {
    scm_ordinal_min = scm_assoc_ref(scm_ordinal, scm_symbol_min);
    scm_ordinal_max = scm_assoc_ref(scm_ordinal, scm_symbol_max);
    ordinal.min =
      (scm_is_integer(scm_ordinal_min) ? scm_to_uintmax(scm_ordinal_min) : 0);
    ordinal.max =
      (scm_is_integer(scm_ordinal_max) ? scm_to_uintmax(scm_ordinal_max) : 0);
    ordinal_pointer = &ordinal;
  } else if (scm_is_integer(scm_ordinal)) {
    ordinal.min = scm_to_uintmax(scm_ordinal);
    ordinal.max = ordinal.min;
    ordinal_pointer = &ordinal;
  } else {
    ordinal_pointer = 0;
  };
  /* retrieve */
  if (scm_is_symbol(scm_retrieve)) {
    if (scm_is_eq(scm_symbol_right, scm_retrieve)) {
      scm_from_relations = scm_from_db_relations_retrieve_right;
    } else if (scm_is_eq(scm_symbol_left, scm_retrieve)) {
      scm_from_relations = scm_from_db_relations_retrieve_left;
    } else if (scm_is_eq(scm_symbol_label, scm_retrieve)) {
      scm_from_relations = scm_from_db_relations_retrieve_label;
    } else if (scm_is_eq(scm_symbol_ordinal, scm_retrieve)) {
      scm_from_relations = scm_from_db_relations_retrieve_ordinal;
    } else {
      status_set_both_goto(status_group_db_guile, status_id_invalid_argument);
    };
  } else {
    scm_from_relations = scm_from_db_relations;
  };
  /* db-relation-select */
  status_require(
    (db_helper_malloc((sizeof(db_guile_relation_selection_t)), (&selection))));
  scm_dynwind_unwind_handler(free, selection, 0);
  memreg_add(selection);
  status_require_read((db_relation_select((*(scm_to_db_txn(scm_txn))),
    left_pointer,
    right_pointer,
    label_pointer,
    ordinal_pointer,
    (&(selection->selection)))));
  selection->left = left;
  selection->right = right;
  selection->label = label;
  selection->scm_from_relations = scm_from_relations;
  selection->status_id = status.id;
  scm_selection = scm_from_db_selection(selection);
  db_status_success_if_notfound;
  db_guile_selection_register(selection, db_guile_selection_type_relation);
exit:
  if (status_is_failure) {
    memreg_free;
  };
  scm_dynwind_end();
  scm_from_status_return(scm_selection);
};
SCM scm_db_record_select(SCM scm_txn,
  SCM scm_type,
  SCM scm_matcher,
  SCM scm_matcher_state) {
  status_declare;
  db_record_matcher_t matcher;
  void* matcher_state;
  SCM scm_selection;
  SCM scm_state;
  db_guile_record_selection_t* selection;
  scm_dynwind_begin(0);
  status_require(
    (db_helper_malloc((sizeof(db_guile_record_selection_t)), (&selection))));
  scm_dynwind_unwind_handler(free, selection, 0);
  /* matcher */
  if (scm_is_true((scm_procedure_p(scm_matcher)))) {
    selection->matcher = scm_cons(scm_matcher,
      (scm_is_undefined(scm_matcher_state)
          ? SCM_EOL
          : (scm_is_true((scm_list_p(scm_matcher_state)))
                ? scm_matcher_state
                : scm_list_1(scm_matcher_state))));
    matcher_state = &(selection->matcher);
    matcher = db_guile_record_matcher;
  } else {
    matcher = 0;
    matcher_state = 0;
  };
  /* record-select */
  status_require_read((db_record_select((*(scm_to_db_txn(scm_txn))),
    (scm_to_db_type(scm_type)),
    matcher,
    matcher_state,
    (&(selection->selection)))));
  selection->status_id = status.id;
  scm_selection = scm_from_db_selection(selection);
  db_guile_selection_register(selection, db_guile_selection_type_record);
exit:
  scm_dynwind_end();
  scm_from_status_return(scm_selection);
};
SCM scm_db_record_ref(SCM scm_type, SCM scm_record, SCM scm_field) {
  db_record_value_t value;
  db_type_t* type;
  db_fields_len_t field_offset;
  type = scm_to_db_type(scm_type);
  field_offset = scm_to_uintmax(scm_field);
  value = db_record_ref(type, (*(scm_to_db_record(scm_record))), field_offset);
  return ((scm_from_field_data(
    (value.data), (value.size), ((field_offset + type->fields)->type))));
};
SCM scm_db_record_to_vector(SCM scm_type, SCM scm_record) {
  db_fields_len_t fields_len;
  db_fields_len_t i;
  SCM result;
  db_type_t* type;
  db_record_value_t value;
  type = scm_to_db_type(scm_type);
  fields_len = type->fields_len;
  result = scm_c_make_vector(fields_len, SCM_BOOL_F);
  for (i = 0; (i < fields_len); i = (1 + i)) {
    value = db_record_ref(type, (*(scm_to_db_record(scm_record))), i);
    scm_c_vector_set_x(result,
      i,
      (scm_from_field_data(
        (value.data), (value.size), ((i + type->fields)->type))));
  };
  return (result);
};
/** allow multiple calls by tracking the record-select return status and
  eventually not calling record-select again */
SCM scm_db_record_read(SCM scm_selection, SCM scm_count) {
  status_declare;
  db_records_t records;
  db_count_t count;
  SCM result;
  db_guile_record_selection_t* selection;
  result = SCM_EOL;
  selection =
    ((db_guile_record_selection_t*)(scm_to_db_selection(scm_selection)));
  if (!(status_id_success == selection->status_id)) {
    return (result);
  };
  count = scm_to_uintmax(scm_count);
  scm_dynwind_begin(0);
  status_require((db_records_new(count, (&records))));
  scm_dynwind_free((records.start));
  status_require_read(
    (db_record_read((selection->selection), count, (&records))));
  selection->status_id = status.id;
  result = scm_from_db_records(records);
exit:
  scm_dynwind_end();
  db_status_success_if_notfound;
  scm_from_status_return(result);
};
SCM scm_db_relation_read(SCM scm_selection, SCM scm_count) {
  status_declare;
  db_relations_t relations;
  db_count_t count;
  db_guile_relation_selection_t* selection;
  SCM result;
  result = SCM_EOL;
  selection = scm_to_db_selection(scm_selection);
  if (!(status_id_success == selection->status_id)) {
    return (result);
  };
  count = scm_to_uintmax(scm_count);
  scm_dynwind_begin(0);
  status_require((db_relations_new(count, (&relations))));
  scm_dynwind_free((relations.start));
  status_require_read(
    (db_relation_read((&(selection->selection)), count, (&relations))));
  selection->status_id = status.id;
  result = (selection->scm_from_relations)(relations);
exit:
  scm_dynwind_end();
  db_status_success_if_notfound;
  scm_from_status_return(result);
};
SCM scm_db_record_update(SCM scm_txn,
  SCM scm_type,
  SCM scm_id,
  SCM scm_values) {
  status_declare;
  db_record_values_declare(values);
  db_type_t* type;
  memreg_register_t allocations;
  type = scm_to_db_type(scm_type);
  scm_dynwind_begin(0);
  debug_log("%d", 4);
  status_require(
    (scm_c_to_db_record_values(type, scm_values, (&values), (&allocations))));
  debug_log("%d", 5);
  scm_dynwind_unwind_handler(
    db_guile_memreg_heap_free, (&allocations), SCM_F_WIND_EXPLICITLY);
  status_require((db_record_update(
    (*(scm_to_db_txn(scm_txn))), (scm_to_uintmax(scm_id)), values)));
exit:
  scm_dynwind_end();
  scm_from_status_return(SCM_UNSPECIFIED);
};
SCM scm_db_record_virtual(SCM scm_type, SCM scm_data) {
  status_declare;
  void* field_data;
  boolean field_data_needs_free;
  size_t field_data_size;
  db_field_type_t field_type;
  db_type_t* type;
  db_id_t id;
  type = scm_to_db_type(scm_type);
  field_type = (*(type->fields)).type;
  /* assumes that no virtual types with invalid field sizes can be created */
  status_require((scm_to_field_data(scm_data,
    field_type,
    (&field_data),
    (&field_data_size),
    (&field_data_needs_free))));
  id = db_record_virtual((type->id), field_data, field_data_size);
  if (field_data_needs_free) {
    free(field_data);
  };
exit:
  scm_from_status_return((scm_from_uintmax(id)));
};
SCM scm_db_record_virtual_data(SCM scm_type, SCM scm_id) {
  status_declare;
  void* data;
  size_t size;
  db_id_t id;
  db_field_type_t field_type;
  db_type_t* type;
  SCM result;
  result = SCM_BOOL_F;
  id = scm_to_uintmax(scm_id);
  type = scm_to_db_type(scm_type);
  size = type->fields->size;
  status_require((db_helper_malloc(size, (&data))));
  data = db_record_virtual_data(id, data, size);
  result = scm_from_field_data(data, size, (type->fields->type));
exit:
  scm_from_status_return(result);
};
SCM scm_db_index_read(SCM scm_txn);
SCM scm_db_record_index_select(SCM scm_txn);
SCM scm_db_record_index_read(SCM scm_txn, SCM scm_count);
/** prepare scm values and register guile bindings */
void db_guile_init() {
  SCM type_slots;
  SCM scm_symbol_data;
  scm_rnrs_raise = scm_c_public_ref("rnrs exceptions", "raise");
  scm_symbol_binary8 = scm_from_latin1_symbol("binary8");
  scm_symbol_binary16 = scm_from_latin1_symbol("binary16");
  scm_symbol_binary32 = scm_from_latin1_symbol("binary32");
  scm_symbol_binary64 = scm_from_latin1_symbol("binary64");
  scm_symbol_string8 = scm_from_latin1_symbol("string8");
  scm_symbol_string16 = scm_from_latin1_symbol("string16");
  scm_symbol_string32 = scm_from_latin1_symbol("string32");
  scm_symbol_string64 = scm_from_latin1_symbol("string64");
  scm_symbol_binary128f = scm_from_latin1_symbol("binary128f");
  scm_symbol_binary16f = scm_from_latin1_symbol("binary16f");
  scm_symbol_binary256f = scm_from_latin1_symbol("binary256f");
  scm_symbol_binary32f = scm_from_latin1_symbol("binary32f");
  scm_symbol_binary64f = scm_from_latin1_symbol("binary64f");
  scm_symbol_binary8f = scm_from_latin1_symbol("binary8f");
  scm_symbol_data = scm_from_latin1_symbol("data");
  scm_symbol_float32f = scm_from_latin1_symbol("float32f");
  scm_symbol_float64f = scm_from_latin1_symbol("float64f");
  scm_symbol_int128f = scm_from_latin1_symbol("int128f");
  scm_symbol_int16f = scm_from_latin1_symbol("int16f");
  scm_symbol_int256f = scm_from_latin1_symbol("int256f");
  scm_symbol_int32f = scm_from_latin1_symbol("int32f");
  scm_symbol_int64f = scm_from_latin1_symbol("int64f");
  scm_symbol_int8f = scm_from_latin1_symbol("int8f");
  scm_symbol_label = scm_from_latin1_symbol("label");
  scm_symbol_left = scm_from_latin1_symbol("left");
  scm_symbol_max = scm_from_latin1_symbol("max");
  scm_symbol_min = scm_from_latin1_symbol("min");
  scm_symbol_ordinal = scm_from_latin1_symbol("ordinal");
  scm_symbol_right = scm_from_latin1_symbol("right");
  scm_symbol_string128f = scm_from_latin1_symbol("string128f");
  scm_symbol_string16f = scm_from_latin1_symbol("string16f");
  scm_symbol_string256f = scm_from_latin1_symbol("string256f");
  scm_symbol_string32f = scm_from_latin1_symbol("string32f");
  scm_symbol_string64f = scm_from_latin1_symbol("string64f");
  scm_symbol_string8f = scm_from_latin1_symbol("string8f");
  scm_symbol_uint128f = scm_from_latin1_symbol("uint128f");
  scm_symbol_uint16f = scm_from_latin1_symbol("uint16f");
  scm_symbol_uint256f = scm_from_latin1_symbol("uint256f");
  scm_symbol_uint32f = scm_from_latin1_symbol("uint32f");
  scm_symbol_uint64f = scm_from_latin1_symbol("uint64f");
  scm_symbol_uint8f = scm_from_latin1_symbol("uint8f");
  type_slots = scm_list_1(scm_symbol_data);
  scm_type_env = scm_make_foreign_object_type(
    (scm_from_latin1_symbol("db-env")), type_slots, 0);
  scm_type_txn = scm_make_foreign_object_type(
    (scm_from_latin1_symbol("db-txn")), type_slots, 0);
  scm_type_record = scm_make_foreign_object_type(
    (scm_from_latin1_symbol("db-record")), type_slots, 0);
  scm_type_selection = scm_make_foreign_object_type(
    (scm_from_latin1_symbol("db-selection")), type_slots, 0);
  type_slots = scm_list_2(scm_symbol_data, (scm_from_latin1_symbol("env")));
  scm_type_type = scm_make_foreign_object_type(
    (scm_from_latin1_symbol("db-type")), type_slots, 0);
  scm_type_index = scm_make_foreign_object_type(
    (scm_from_latin1_symbol("db-index")), type_slots, 0);
  scm_c_define_procedure_c_init;
  SCM m = scm_c_resolve_module("sph db");
  scm_c_module_define(
    m, "db-type-flag-virtual", (scm_from_uint8(db_type_flag_virtual)));
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
  scm_c_define_procedure_c("db-type-create", 3, 1, 0, scm_db_type_create, "");
  scm_c_define_procedure_c("db-type-delete", 2, 0, 0, scm_db_type_delete, "");
  scm_c_define_procedure_c("db-type-get", 2, 0, 0, scm_db_type_get, "");
  scm_c_define_procedure_c("db-type-id", 1, 0, 0, scm_db_type_id, "");
  scm_c_define_procedure_c("db-type-name", 1, 0, 0, scm_db_type_name, "");
  scm_c_define_procedure_c("db-type-indices", 1, 0, 0, scm_db_type_indices, "");
  scm_c_define_procedure_c("db-type-fields", 1, 0, 0, scm_db_type_fields, "");
  scm_c_define_procedure_c(
    "db-type-virtual?", 1, 0, 0, scm_db_type_virtual_p, "");
  scm_c_define_procedure_c("db-type-flags", 1, 0, 0, scm_db_type_flags, "");
  scm_c_define_procedure_c("db-index-create", 3, 0, 0, scm_db_index_create, "");
  scm_c_define_procedure_c("db-index-delete",
    2,
    0,
    0,
    scm_db_index_delete,
    ("env index -> unspecified"));
  scm_c_define_procedure_c("db-index-get",
    3,
    0,
    0,
    scm_db_index_get,
    ("env type fields:(integer:offset ...) -> index"));
  scm_c_define_procedure_c("db-index-rebuild",
    2,
    0,
    0,
    scm_db_index_rebuild,
    ("env index -> unspecified"));
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
  scm_c_define_procedure_c("db-relation-select",
    1,
    5,
    0,
    scm_db_relation_select,
    ("db-txn [list:left list:right list:label retrieve ordinal]"));
  scm_c_define_procedure_c("db-relation-read",
    2,
    0,
    0,
    scm_db_relation_read,
    "selection integer:count");
  scm_c_define_procedure_c("db-record-select",
    2,
    2,
    0,
    scm_db_record_select,
    ("txn type [matcher matcher-state]"));
  scm_c_define_procedure_c(
    "db-record-read", 2, 0, 0, scm_db_record_read, "selection integer:count");
  scm_c_define_procedure_c(
    "db-record-ref", 3, 0, 0, scm_db_record_ref, "type record field:integer");
  scm_c_define_procedure_c("db-record-get",
    2,
    0,
    0,
    scm_db_record_get,
    ("txn list:ids -> (db-record ...)"));
  scm_c_define_procedure_c(("db-record->vector"),
    2,
    0,
    0,
    scm_db_record_to_vector,
    ("type db-record -> vector:#(any:value ...)"));
  scm_c_define_procedure_c("db-record-update",
    4,
    0,
    0,
    scm_db_record_update,
    ("db-txn db-type id list:((field-id . value) ...) -> unspecified"));
  scm_c_define_procedure_c(
    "db-record-virtual", 2, 0, 0, scm_db_record_virtual, ("type data -> id"));
  scm_c_define_procedure_c("db-record-virtual-data",
    2,
    0,
    0,
    scm_db_record_virtual_data,
    ("db-env id -> any:data"));
};