
#ifndef dg_data_integer_type
#define dg_data_integer_type int64
#endif
#ifndef dg_guile_intern_type_size
#define dg_guile_intern_type_size 1
#endif
#define debug_log_p 1
#define scm_enable_typechecks_p 1
#include <libguile.h>
#include <sph-dg.c>
#ifndef sc_included_sph_one
#define sc_included_sph_one
#ifndef sc_included_string_h
#include <string.h>
#define sc_included_string_h
#endif
#ifndef sc_included_stdlib_h
#include <stdlib.h>
#define sc_included_stdlib_h
#endif
#ifndef sc_included_unistd_h
#include <unistd.h>
#define sc_included_unistd_h
#endif
#ifndef sc_included_sys_stat_h
#include <sys/stat.h>
#define sc_included_sys_stat_h
#endif
#ifndef sc_included_libgen_h
#include <libgen.h>
#define sc_included_libgen_h
#endif
#ifndef sc_included_errno_h
#include <errno.h>
#define sc_included_errno_h
#endif
#define file_exists_p(path) !(access(path, F_OK) == -1)
#define pointer_equal_p(a, b) (((b0 *)(a)) == ((b0 *)(b)))
#define free_and_set_zero(a)                                                   \
  free(a);                                                                     \
  a = 0
#define increment(a) a = (1 + a)
#define decrement(a) a = (a - 1)
/** set result to a new string with a trailing slash added, or the given string
  if it already has a trailing slash. returns 0 if result is the given string, 1
  if new memory could not be allocated, 2 if result is a new string */
b8 ensure_trailing_slash(b8 *a, b8 **result) {
  b32 a_len = strlen(a);
  if ((!a_len || (('/' == (*(a + (a_len - 1))))))) {
    (*result) = a;
    return (0);
  } else {
    char *new_a = malloc((2 + a_len));
    if (!new_a) {
      return (1);
    };
    memcpy(new_a, a, a_len);
    memcpy((new_a + a_len), "/", 1);
    (*(new_a + (1 + a_len))) = 0;
    (*result) = new_a;
    return (2);
  };
};
/** return a new string with the same contents as the given string. return 0 if
 * the memory allocation failed */
b8 *string_clone(b8 *a) {
  size_t a_size = (1 + strlen(a));
  b8 *result = malloc(a_size);
  if (result) {
    memcpy(result, a, a_size);
  };
  return (result);
};
/** like posix dirname, but never modifies its argument and always returns a new
 * string */
b8 *dirname_2(b8 *a) {
  b8 *path_copy = string_clone(a);
  return (dirname(path_copy));
};
/** return 1 if the path exists or has been successfully created */
boolean ensure_directory_structure(b8 *path, mode_t mkdir_mode) {
  if (file_exists_p(path)) {
    return (1);
  } else {
    b8 *path_dirname = dirname_2(path);
    boolean status = ensure_directory_structure(path_dirname, mkdir_mode);
    free(path_dirname);
    return ((status &&
             ((((EEXIST == errno)) || ((0 == mkdir(path, mkdir_mode)))))));
  };
};
/** always returns a new string */
b8 *string_append(b8 *a, b8 *b) {
  size_t a_length = strlen(a);
  size_t b_length = strlen(b);
  b8 *result = malloc((1 + a_length + b_length));
  if (result) {
    memcpy(result, a, a_length);
    memcpy((result + a_length), b, (1 + b_length));
  };
  return (result);
};
#endif
#ifndef sc_included_sph_guile
#define sc_included_sph_guile
#define scm_first SCM_CAR
#define scm_tail SCM_CDR
#define scm_c_define_procedure_c_init SCM scm_c_define_procedure_c_temp
#define scm_is_undefined(a) (SCM_UNDEFINED == a)
#define scm_c_define_procedure_c(name, required, optional, rest, c_function,   \
                                 documentation)                                \
  scm_c_define_procedure_c_temp =                                              \
      scm_c_define_gsubr(name, required, optional, rest, c_function);          \
  scm_set_procedure_property_x(scm_c_define_procedure_c_temp,                  \
                               scm_from_locale_symbol("documentation"),        \
                               scm_from_locale_string(documentation))
b0 scm_debug_log(SCM value) {
  scm_call_2(scm_variable_ref(scm_c_lookup("write")), value,
             scm_current_output_port());
  scm_newline(scm_current_output_port());
};
SCM scm_c_bytevector_take(size_t size_octets, b8 *a) {
  SCM r = scm_c_make_bytevector(size_octets);
  memcpy(SCM_BYTEVECTOR_CONTENTS(r), a, size_octets);
  return (r);
};
#define scm_c_list_each(list, e, body)                                         \
  while (!scm_is_null(list)) {                                                 \
    e = scm_first(list);                                                       \
    body;                                                                      \
    list = scm_tail(list);                                                     \
  }
#endif
#define status_to_scm_return(result) return (status_to_scm(result))
#define optional_count(a) (scm_is_integer(a) ? scm_to_uint(a) : 0)
#define optional_count_one(a) (scm_is_integer(a) ? scm_to_uint(a) : 1)
#define optional_every_p(a) (scm_is_undefined(a) || scm_is_true(a))
#define optional_types(a) (scm_is_integer(a) ? scm_to_uint8(a) : 0)
#define optional_offset(a) (scm_is_integer(a) ? scm_to_uint32(a) : 0)
#define scm_to_txn(a) ((dg_txn_t *)(SCM_SMOB_DATA(a)))
#define dg_id_to_scm(a) scm_from_uint(a)
#define scm_to_dg_id(a) scm_to_uint(a)
#define dg_guile_scm_to_ordinal scm_to_int
#define dg_guile_intern_bytevector 0
#define dg_guile_intern_integer 1
#define dg_guile_intern_string 2
#define dg_guile_intern_rational 3
#define dg_guile_intern_scheme 4
/** append ids from scm-a to dg-ids-t* list if a is not false or undefined (no
 * filter) */
#define optional_ids(scm_a, a)                                                 \
  if (scm_is_pair(scm_a)) {                                                    \
    status_require_x(scm_to_dg_ids(scm_a, &a));                                \
  }
;
#define optional_relation_retrieve(a)                                            \
  (scm_is_symbol(a)                                                              \
       ? (scm_is_eq(scm_symbol_right, a)                                         \
              ? dg_relation_records_to_scm_retrieve_right                        \
              : (scm_is_eq(scm_symbol_left, a)                                   \
                     ? dg_relation_records_to_scm_retrieve_left                  \
                     : (scm_is_eq(scm_symbol_label, a)                           \
                            ? dg_relation_records_to_scm_retrieve_label          \
                            : (scm_is_eq(scm_symbol_ordinal, a)                  \
                                   ? dg_relation_records_to_scm_retrieve_ordinal \
                                   : 0))))                                       \
       : dg_relation_records_to_scm)
#define txn_to_scm(txn_pointer)                                                \
  scm_new_smob(scm_type_txn, ((scm_t_bits)(txn_pointer)))
typedef enum {
  dg_read_state_type_relation,
  dg_read_state_type_node
} dg_read_state_type_t;
typedef struct {
  dg_ids_t *left;
  dg_ids_t *right;
  dg_ids_t *label;
  SCM (*records_to_scm)(dg_relation_records_t *);
  dg_relation_read_state_t dg_state;
} dg_guile_relation_read_state_t;
typedef struct {
  b0 *state;
  dg_read_state_type_t read_state_type;
} dg_guile_generic_read_state_t;
#define mi_list_name_prefix dg_guile_generic_read_states
#define mi_list_element_t dg_guile_generic_read_state_t
#ifndef sc_included_sph_mi_list
#define sc_included_sph_mi_list
/* a minimal linked list with custom element types.
   this file can be included multiple times to create differently typed
   versions, depending the value of the preprocessor variables
   mi-list-name-infix and mi-list-element-t before inclusion */
#ifndef sc_included_stdlib_h
#include <stdlib.h>
#define sc_included_stdlib_h
#endif
#ifndef sc_included_inttypes_h
#include <inttypes.h>
#define sc_included_inttypes_h
#endif
#ifndef mi_list_name_prefix
#define mi_list_name_prefix mi_list_64
#endif
#ifndef mi_list_element_t
#define mi_list_element_t uint64_t
#endif
/* there does not seem to be a simpler way for identifier concatenation in c in
 * this case */
#ifndef mi_list_name_concat
#define mi_list_name_concat(a, b) a##_##b
#define mi_list_name_concatenator(a, b) mi_list_name_concat(a, b)
#define mi_list_name(name) mi_list_name_concatenator(mi_list_name_prefix, name)
#endif
#define mi_list_struct_name mi_list_name(struct)
#define mi_list_t mi_list_name(t)
typedef struct mi_list_struct_name {
  struct mi_list_struct_name *link;
  mi_list_element_t data;
} mi_list_t;
#ifndef mi_list_first
#define mi_list_first(a) (*a).data
#define mi_list_first_address(a) &(*a).data
#define mi_list_rest(a) (*a).link
#endif
mi_list_t *mi_list_name(drop)(mi_list_t *a) {
  mi_list_t *a_next = mi_list_rest(a);
  free(a);
  return (a_next);
};
/** it would be nice to set the pointer to zero, but that would require more
 * indirection with a pointer-pointer */
void mi_list_name(destroy)(mi_list_t *a) {
  mi_list_t *a_next = 0;
  while (a) {
    a_next = (*a).link;
    free(a);
    a = a_next;
  };
};
mi_list_t *mi_list_name(add)(mi_list_t *a, mi_list_element_t value) {
  mi_list_t *element = calloc(1, sizeof(mi_list_t));
  if (!element) {
    return (0);
  };
  (*element).data = value;
  (*element).link = a;
  return (element);
};
size_t mi_list_name(length)(mi_list_t *a) {
  size_t result = 0;
  while (a) {
    result = (1 + result);
    a = mi_list_rest(a);
  };
  return (result);
};
#undef mi_list_name_prefix
#undef mi_list_element_t
#undef mi_list_struct_name
#undef mi_list_t

#endif
#define dg_guile_generic_read_states_first mi_list_first
#define dg_guile_generic_read_states_rest mi_list_rest
__thread dg_guile_generic_read_states_t *active_read_states = 0;
/** create a generic-read-state object with the given state and read-state-type
  and add it to the thread-local active-read-states list */
#define active_read_states_add_x(state, read_state_type)                       \
  dg_guile_generic_read_state_t generic_read_state = {state, read_state_type}; \
  dg_guile_generic_read_states_t *generic_read_states_temp =                   \
      dg_guile_generic_read_states_add(active_read_states,                     \
                                       generic_read_state);                    \
  if (generic_read_states_temp) {                                              \
    active_read_states = generic_read_states_temp;                             \
  } else {                                                                     \
    status_set_both(dg_status_group_dg, dg_status_id_memory);                  \
  }
;
/** read states are freed when the transaction is finalised. there can only be
  one transaction per thread. dg read states may be wrapped in dg-guile read
  states to carry pointers to values that are to be garbage collected */
b0 active_read_states_free() {
  dg_guile_generic_read_state_t generic_read_state;
  while (active_read_states) {
    generic_read_state = dg_guile_generic_read_states_first(active_read_states);
    if ((dg_read_state_type_relation == generic_read_state.read_state_type)) {
      dg_guile_relation_read_state_t *state = generic_read_state.state;
      dg_relation_selection_destroy(&(*state).dg_state);
      dg_ids_destroy((*state).left);
      dg_ids_destroy((*state).label);
      dg_ids_destroy((*state).right);
      free(state);
    } else {
      if ((dg_read_state_type_node == generic_read_state.read_state_type)) {
        dg_node_read_state_t *state = generic_read_state.state;
        dg_node_selection_destroy(state);
      };
    };
    active_read_states = dg_guile_generic_read_states_drop(active_read_states);
  };
};
scm_t_bits scm_type_selection;
scm_t_bits scm_type_txn;
SCM dg_scm_write;
SCM dg_scm_read;
SCM scm_rnrs_raise;
SCM scm_symbol_label;
SCM scm_symbol_left;
SCM scm_symbol_right;
SCM scm_symbol_ordinal;
/** with gcc optimisation level 3, not using a local variable did not set the
  smob data. passing a null pointer creates an empty/null selection */
SCM selection_to_scm(b0 *pointer) {
  SCM result = scm_new_smob(scm_type_selection, ((scm_t_bits)(pointer)));
  return (result);
};
#define scm_to_selection(a, type_group_name)                                   \
  ((dg_##type_group_name##_read_state_t *)(SCM_SMOB_DATA(a)))
#define scm_c_error(name, description)                                         \
  scm_call_1(scm_rnrs_raise,                                                   \
             scm_list_3(scm_from_latin1_symbol(name),                          \
                        scm_cons(scm_from_latin1_symbol("description"),        \
                                 scm_from_utf8_string(description)),           \
                        scm_cons(scm_from_latin1_symbol("c-routine"),          \
                                 scm_from_latin1_symbol(__FUNCTION__))))
#define status_to_scm_error(a)                                                 \
  scm_c_error(dg_status_name(a), dg_status_description(a))
#define status_to_scm(result)                                                  \
  (status_success_p ? result : status_to_scm_error(status))
#define define_dg_type_p(name)                                                 \
  SCM scm_dg_##name##_p(SCM id) {                                              \
    return (scm_from_bool(                                                     \
        (scm_is_integer(id) && dg_##name##_p(scm_to_uint(id)))));              \
  }
#define scm_string_octet_length_uint(a)                                        \
  scm_to_uint(scm_product(scm_string_bytes_per_char(a), scm_string_length(a)))
#define dg_pre_concat_primitive(a, b) a##b
#define dg_pre_concat(a, b) dg_pre_concat_primitive(a, b)
#define dg_scm_to_data_integer dg_pre_concat(scm_to_, dg_data_integer_type)
#define dg_data_integer_to_scm dg_pre_concat(scm_from_, dg_data_integer_type)
#define dg_data_integer_t dg_pre_concat(dg_data_integer_type, _t)
#define scm_c_alist_add_from_struct(target, source, key, struct_key, convert)  \
  target = scm_acons(scm_from_latin1_symbol(key), convert(source.struct_key),  \
                     target)
SCM scm_bytevector_null;
/** strings are stored without a trailing 0 because we have the octet size
 * exactly */
status_t scm_string_to_dg_data(SCM a, dg_data_t *result, b8 intern_type) {
  status_init;
  size_t a_size;
  b8 *a_c = scm_to_utf8_stringn(a, &a_size);
  size_t size = (dg_guile_intern_type_size + a_size);
  b8 *data = calloc(size, 1);
  if (!data) {
    dg_status_set_id_goto(dg_status_id_memory);
  };
  memcpy((dg_guile_intern_type_size + data), a_c,
         (size - dg_guile_intern_type_size));
#if dg_guile_intern_type_size
  (*data) = intern_type;
#endif
  (*result).data = data;
  (*result).size = size;
exit:
  return (status);
};
/** the caller has to free the data field in the result struct */
status_t scm_to_dg_data(SCM a, dg_data_t *result) {
  status_init;
  if (scm_is_bytevector(a)) {
    size_t size = (dg_guile_intern_type_size + SCM_BYTEVECTOR_LENGTH(a));
    b8 *data = calloc(size, 1);
    if (!data) {
      dg_status_set_id_goto(dg_status_id_memory);
    };
#if dg_guile_intern_type_size
    (*data) = dg_guile_intern_bytevector;
#endif
    memcpy((dg_guile_intern_type_size + data), SCM_BYTEVECTOR_CONTENTS(a),
           size);
    (*result).data = data;
    (*result).size = size;
  } else {
    if (scm_is_string(a)) {
      scm_string_to_dg_data(a, result, dg_guile_intern_string);
    } else {
      if (scm_is_integer(a)) {
        size_t size = (dg_guile_intern_type_size + sizeof(dg_data_integer_t));
        b8 *data = calloc(size, 1);
        if (!data) {
          dg_status_set_id_goto(dg_status_id_memory);
        };
#if dg_guile_intern_integer
        (*data) = dg_guile_intern_integer;
#endif
        (*((dg_data_integer_t *)((dg_guile_intern_type_size + data)))) =
            dg_scm_to_data_integer(a);
        (*result).data = data;
        (*result).size = size;
      } else {
        if (scm_is_rational(a)) {
          size_t size = (dg_guile_intern_type_size + sizeof(double));
          b8 *data = calloc(size, 1);
          if (!data) {
            dg_status_set_id_goto(dg_status_id_memory);
          };
#if dg_guile_intern_type_size
          (*data) = dg_guile_intern_rational;
#endif
          (*((double *)((dg_guile_intern_type_size + data)))) =
              scm_to_double(a);
        } else {
          SCM b = scm_object_to_string(a, dg_scm_write);
          scm_string_to_dg_data(b, result, dg_guile_intern_scheme);
        };
      };
    };
  };
exit:
  return (status);
};
b0 dg_data_list_data_free(dg_data_list_t *a) {
  while (a) {
    free(dg_data_list_first(a).data);
    a = dg_data_list_rest(a);
  };
};
b0 debug_display_data(b8 *a, size_t size) {
  if (!size) {
    return;
  };
  printf("%x", (*(a + 0)));
  size_t index = 1;
  while ((index < size)) {
    printf(" %x", (*(a + index)));
    index = (1 + index);
  };
  printf("\n");
};
#define define_dg_data_to_scm(type_group_name)                                 \
  SCM dg_##type_group_name##_to_scm_bytevector(dg_##type_group_name##_t a) {   \
    SCM r = scm_c_make_bytevector((a.size - dg_guile_intern_type_size));       \
    memcpy(SCM_BYTEVECTOR_CONTENTS(r),                                         \
           (dg_guile_intern_type_size + ((b8 *)(a.data))),                     \
           (a.size - dg_guile_intern_type_size));                              \
    return (r);                                                                \
  };                                                                           \
  SCM dg_##type_group_name##_to_scm_string(dg_##type_group_name##_t a) {       \
    scm_from_utf8_stringn((dg_guile_intern_type_size + ((b8 *)(a.data))),      \
                          (a.size - dg_guile_intern_type_size));               \
  };                                                                           \
  SCM dg_##type_group_name##_to_scm_integer(dg_##type_group_name##_t a) {      \
    if ((a.size > dg_guile_intern_type_size)) {                                \
      dg_data_integer_to_scm((*((dg_data_integer_t *)((                        \
          dg_guile_intern_type_size + ((b8 *)(a.data)))))));                   \
    } else {                                                                   \
      scm_from_int8(0);                                                        \
    };                                                                         \
  };                                                                           \
  SCM dg_##type_group_name##_to_scm_rational(dg_##type_group_name##_t a) {     \
    if ((a.size > dg_guile_intern_type_size)) {                                \
      scm_from_double(                                                         \
          (*((double *)((dg_guile_intern_type_size + ((b8 *)(a.data)))))));    \
    } else {                                                                   \
      scm_from_int8(0);                                                        \
    };                                                                         \
  };                                                                           \
  SCM dg_##type_group_name##_to_scm_scheme(dg_##type_group_name##_t a) {       \
    scm_call_with_input_string(                                                \
        scm_from_utf8_stringn((dg_guile_intern_type_size + ((b8 *)(a.data))),  \
                              (a.size - dg_guile_intern_type_size)),           \
        dg_scm_read);                                                          \
  };                                                                           \
  SCM dg_##type_group_name##_to_scm(dg_##type_group_name##_t a) {              \
    b8 type = (dg_guile_intern_type_size ? (*((b8 *)(a.data)))                 \
                                         : dg_guile_intern_bytevector);        \
    ((dg_guile_intern_bytevector == type)                                      \
         ? dg_##type_group_name##_to_scm_bytevector                            \
         : ((dg_guile_intern_integer == type)                                  \
                ? dg_##type_group_name##_to_scm_integer                        \
                : ((dg_guile_intern_string == type)                            \
                       ? dg_##type_group_name##_to_scm_string                  \
                       : ((dg_guile_intern_rational == type)                   \
                              ? dg_##type_group_name##_to_scm_rational         \
                              : ((dg_guile_intern_scheme == type)              \
                                     ? dg_##type_group_name##_to_scm_scheme    \
                                     : 0)))))(a);                              \
  }
define_dg_data_to_scm(data);
define_dg_data_to_scm(data_record);
SCM dg_ids_to_scm(dg_ids_t *a) {
  SCM result = SCM_EOL;
  while (a) {
    result = scm_cons(dg_id_to_scm(dg_ids_first(a)), result);
    a = dg_ids_rest(a);
  };
  return (result);
};
status_t scm_to_dg_ids(SCM a, dg_ids_t **result) {
  status_init;
  dg_ids_t *result_temp = (*result);
  while (!scm_is_null(a)) {
    result_temp = dg_ids_add(result_temp, scm_to_dg_id(scm_first(a)));
    if (result_temp) {
      (*result) = result_temp;
      a = scm_tail(a);
    } else {
      dg_ids_destroy((*result));
      dg_status_set_id_goto(dg_status_id_memory);
    };
  };
exit:
  return (status);
};
SCM dg_data_list_to_scm(dg_data_list_t *a) {
  SCM result = SCM_EOL;
  while (a) {
    result = scm_cons(dg_data_to_scm(dg_data_list_first(a)), result);
    a = dg_data_list_rest(a);
  };
  return (result);
};
SCM dg_data_records_to_scm(dg_data_records_t *a,
                           SCM (*convert_data)(dg_data_record_t)) {
  SCM result = SCM_EOL;
  dg_data_record_t record;
  SCM data;
  while (a) {
    record = dg_data_records_first(a);
    data = (record.size ? convert_data(record) : scm_bytevector_null);
    result =
        scm_cons(scm_vector(scm_list_2(dg_id_to_scm(record.id), data)), result);
    a = dg_data_records_rest(a);
  };
  return (result);
};
SCM dg_relation_records_to_scm(dg_relation_records_t *a) {
  SCM result = SCM_EOL;
  dg_relation_record_t record;
  while (a) {
    record = dg_relation_records_first(a);
    result =
        scm_cons(scm_vector(scm_list_4(
                     dg_id_to_scm(record.left), dg_id_to_scm(record.right),
                     dg_id_to_scm(record.label), dg_id_to_scm(record.ordinal))),
                 result);
    a = dg_relation_records_rest(a);
  };
  return (result);
};
#define define_dg_relation_records_to_scm_retrieve(field_name)                 \
  SCM dg_relation_records_to_scm_retrieve_##field_name(                        \
      dg_relation_records_t *a) {                                              \
    SCM result = SCM_EOL;                                                      \
    dg_relation_record_t record;                                               \
    while (a) {                                                                \
      record = dg_relation_records_first(a);                                   \
      result = scm_cons(dg_id_to_scm(record.field_name), result);              \
      a = dg_relation_records_rest(a);                                         \
    };                                                                         \
    return (result);                                                           \
  }
define_dg_relation_records_to_scm_retrieve(left);
define_dg_relation_records_to_scm_retrieve(right);
define_dg_relation_records_to_scm_retrieve(label);
define_dg_relation_records_to_scm_retrieve(ordinal);
status_t scm_to_dg_data_list(SCM a, dg_data_list_t **result) {
  status_init;
  dg_data_list_t *result_temp = (*result);
  dg_data_t data_temp = {0, 0};
  while (!scm_is_null(a)) {
    status_require_x(scm_to_dg_data(scm_first(a), &data_temp));
    result_temp = dg_data_list_add(result_temp, data_temp);
    if (result_temp) {
      (*result) = result_temp;
      a = scm_tail(a);
    } else {
      dg_status_set_id_goto(dg_status_id_memory);
    };
  };
exit:
  if (status_failure_p) {
    free(data_temp.data);
  };
  return (status);
};
SCM scm_from_mdb_stat(MDB_stat *a) {
  status_init;
  SCM result = SCM_EOL;
#define result_add(key, struct_key)                                            \
  scm_c_alist_add_from_struct(result, (*a), key, struct_key, scm_from_uint)
  result_add("ms-psize", ms_psize);
  result_add("ms-depth", ms_depth);
  result_add("ms-branch-pages", ms_branch_pages);
  result_add("ms-leaf-pages", ms_leaf_pages);
  result_add("ms-overflow-pages", ms_overflow_pages);
  result_add("ms-entries", ms_entries);
#undef result_add
  status_to_scm_return(result);
};
SCM scm_dg_init(SCM scm_path, SCM scm_options) {
  status_init;
  dg_init_options_t options;
  dg_init_options_t *options_pointer;
  SCM scm_temp;
  b8 *path = 0;
  if ((scm_is_undefined(scm_options) || scm_is_null(scm_options))) {
    options_pointer = 0;
  } else {
#define scm_get_value(name)                                                    \
  scm_temp = scm_assoc_ref(scm_options, scm_from_latin1_symbol(name));         \
  scm_temp = (scm_is_pair(scm_temp) ? scm_tail(scm_temp) : SCM_UNDEFINED)
    dg_init_options_set_defaults(&options);
    scm_get_value("read-only?");
    if (scm_is_bool(scm_temp)) {
      options.read_only_p = scm_is_true(scm_temp);
    };
    scm_get_value("maximum-size-octets");
    if (scm_is_integer(scm_temp)) {
      options.maximum_size_octets = scm_to_uint(scm_temp);
    };
    scm_get_value("maximum-reader-count");
    if (scm_is_integer(scm_temp)) {
      options.maximum_reader_count = scm_to_uint(scm_temp);
    };
    scm_get_value("filesystem-has-ordered-writes?");
    if (scm_is_bool(scm_temp)) {
      options.filesystem_has_ordered_writes_p = scm_is_true(scm_temp);
    };
    scm_get_value("env-open-flags");
    if (scm_is_integer(scm_temp)) {
      options.env_open_flags = scm_to_uint(scm_temp);
    };
    scm_get_value("file-permissions");
    if (scm_is_integer(scm_temp)) {
      options.file_permissions = scm_to_uint(scm_temp);
    };
    options_pointer = &options;
#undef scm_get_value
  };
  path = scm_to_locale_string(scm_path);
  status_require_x(dg_init(path, options_pointer));
  SCM scm_module = scm_c_resolve_module("sph storage dg");
  scm_temp =
      scm_variable_ref(scm_c_module_lookup(scm_module, "dg-init-extension"));
  while (!scm_is_null(scm_temp)) {
    scm_call_0(scm_first(scm_temp));
    scm_temp = scm_tail(scm_temp);
  };
exit:
  free(path);
  status_to_scm_return(SCM_BOOL_T);
};
SCM scm_dg_exit() {
  scm_gc();
  dg_exit();
  return (SCM_UNSPECIFIED);
};
SCM scm_dg_initialised_p() { return (scm_from_bool(dg_initialised)); };
SCM scm_dg_root() { return (scm_from_locale_string(dg_root)); };
#define define_scm_dg_txn_create(name, flags)                                  \
  SCM scm_dg_txn_create_##name() {                                             \
    status_init;                                                               \
    dg_txn_t *txn;                                                             \
    dg_mdb_status_require_x(mdb_txn_begin(dg_mdb_env, 0, flags, &txn));        \
    SCM result = txn_to_scm(txn);                                              \
  exit:                                                                        \
    if ((result && status_failure_p)) {                                        \
      free(txn);                                                               \
    };                                                                         \
    status_to_scm_return(result);                                              \
  }
define_scm_dg_txn_create(read, MDB_RDONLY);
define_scm_dg_txn_create(write, 0);
define_dg_type_p(id);
define_dg_type_p(intern);
define_dg_type_p(extern);
define_dg_type_p(relation);
SCM scm_dg_txn_abort(SCM scm_txn) {
  active_read_states_free();
  mdb_txn_abort(scm_to_txn(scm_txn));
  SCM_SET_SMOB_DATA(scm_txn, 0);
  return (SCM_UNSPECIFIED);
};
/** note that mdb-txn-commit frees cursors - active-read-states-free uses
  mdb-cursor-close. if active-read-states-free is called after mdb-txn-commit a
  double free occurs */
SCM scm_dg_txn_commit(SCM scm_txn) {
  status_init;
  active_read_states_free();
  dg_mdb_status_require_x(mdb_txn_commit(scm_to_txn(scm_txn)));
  SCM_SET_SMOB_DATA(scm_txn, 0);
exit:
  status_to_scm_return(SCM_UNSPECIFIED);
};
SCM scm_dg_id_create(SCM scm_txn, SCM scm_count) {
  status_init;
  b32 count = optional_count_one(scm_count);
  dg_ids_t *ids = 0;
  status_require_x(dg_id_create(scm_to_txn(scm_txn), count, &ids));
  SCM result = dg_ids_to_scm(ids);
exit:
  dg_ids_destroy(ids);
  status_to_scm_return(result);
};
SCM scm_dg_extern_create(SCM scm_txn, SCM scm_count, SCM scm_data) {
  status_init;
  b32 count = optional_count_one(scm_count);
  dg_data_t data_struct = {0, 0};
  dg_data_t *data = &data_struct;
  if (scm_is_undefined(scm_data)) {
    data = 0;
  } else {
    status_require_x(scm_to_dg_data(scm_data, data));
  };
  dg_ids_t *ids = 0;
  status_require_x(dg_extern_create(scm_to_txn(scm_txn), count, data, &ids));
  SCM result = dg_ids_to_scm(ids);
exit:
  dg_ids_destroy(ids);
  free(data_struct.data);
  status_to_scm_return(result);
};
SCM scm_dg_extern_id_to_data(SCM scm_txn, SCM scm_ids, SCM scm_every_p) {
  status_init;
  boolean every_p = optional_every_p(scm_every_p);
  dg_ids_t *ids = 0;
  status_require_x(scm_to_dg_ids(scm_ids, &ids));
  dg_data_list_t *data = 0;
  dg_status_require_read_x(
      dg_extern_id_to_data(scm_to_txn(scm_txn), ids, every_p, &data));
  SCM result = dg_data_list_to_scm(data);
exit:
  dg_ids_destroy(ids);
  dg_data_list_destroy(data);
  status_to_scm_return(result);
};
SCM scm_dg_extern_data_to_id(SCM scm_txn, SCM scm_data) {
  status_init;
  dg_data_t data = {0, 0};
  status_require_x(scm_to_dg_data(scm_data, &data));
  dg_ids_t *ids = 0;
  dg_status_require_read_x(
      dg_extern_data_to_id(scm_to_txn(scm_txn), data, &ids));
  SCM result = dg_ids_to_scm(ids);
exit:
  dg_ids_destroy(ids);
  free(data.data);
  status_to_scm_return(result);
};
dg_ordinal_t dg_guile_ordinal_generator(b0 *state) {
  SCM scm_state = (*((SCM *)(state)));
  SCM scm_generator = scm_first(scm_state);
  SCM scm_result = scm_apply_0(scm_generator, scm_tail(scm_state));
  (*((SCM *)(state))) = scm_cons(scm_generator, scm_result);
  return (dg_guile_scm_to_ordinal(scm_first(scm_result)));
};
SCM scm_dg_relation_ensure(SCM scm_txn, SCM scm_left, SCM scm_right,
                           SCM scm_label, SCM scm_ordinal_generator,
                           SCM scm_ordinal_generator_state) {
  status_init;
  if ((scm_is_undefined(scm_label) || !scm_is_true(scm_label))) {
    scm_label = scm_list_1(scm_from_uint8(0));
  };
  dg_define_ids_3(left, right, label);
  status_require_x(scm_to_dg_ids(scm_left, &left));
  status_require_x(scm_to_dg_ids(scm_right, &right));
  if (scm_is_true(scm_label)) {
    status_require_x(scm_to_dg_ids(scm_label, &label));
  };
  dg_relation_ordinal_generator_t ordinal_generator = 0;
  b0 *ordinal_generator_state;
  dg_ordinal_t ordinal_value;
  SCM scm_state;
  if (scm_is_true(scm_procedure_p(scm_ordinal_generator))) {
    scm_state = scm_cons(scm_ordinal_generator,
                         (scm_is_true(scm_list_p(scm_ordinal_generator_state))
                              ? scm_ordinal_generator_state
                              : scm_list_1(scm_ordinal_generator_state)));
    ordinal_generator_state = &scm_state;
    ordinal_generator = dg_guile_ordinal_generator;
  } else {
    ordinal_value = (scm_is_undefined(scm_ordinal_generator_state)
                         ? 0
                         : scm_to_uint(scm_ordinal_generator_state));
    ordinal_generator_state = &ordinal_value;
  };
  status_require_x(dg_relation_ensure(scm_to_txn(scm_txn), left, right, label,
                                      ordinal_generator,
                                      ordinal_generator_state));
exit:
  dg_ids_destroy(left);
  dg_ids_destroy(right);
  dg_ids_destroy(label);
  status_to_scm_return(SCM_BOOL_T);
};
SCM scm_dg_statistics(SCM scm_txn) {
  status_init;
  SCM result = SCM_EOL;
  dg_statistics_t stat;
  status_require_x(dg_statistics(scm_to_txn(scm_txn), &stat));
#define result_add(key, struct_key)                                            \
  result = scm_acons(scm_from_latin1_symbol(key),                              \
                     scm_from_mdb_stat(&stat.struct_key), result)
  result_add("id->data", id_to_data);
  result_add("data-intern->id", data_intern_to_id);
  result_add("data-extern->extern", data_extern_to_extern);
  result_add("left->right", left_to_right);
  result_add("right->left", right_to_left);
  result_add("label->left", label_to_left);
#undef result_add
exit:
  status_to_scm_return(result);
};
SCM scm_dg_delete(SCM scm_txn, SCM scm_ids) {
  status_init;
  dg_ids_t *ids = 0;
  status_require_x(scm_to_dg_ids(scm_ids, &ids));
  status_require_x(dg_delete(scm_to_txn(scm_txn), ids));
exit:
  dg_ids_destroy(ids);
  status_to_scm_return(SCM_UNSPECIFIED);
};
SCM scm_dg_identify(SCM scm_txn, SCM scm_ids) {
  status_init;
  dg_ids_t *ids = 0;
  status_require_x(scm_to_dg_ids(scm_ids, &ids));
  dg_ids_t *ids_result = 0;
  status_require_x(dg_identify(scm_to_txn(scm_txn), ids, &ids_result));
  SCM result = dg_ids_to_scm(ids_result);
exit:
  dg_ids_destroy(ids);
  dg_ids_destroy(ids_result);
  status_to_scm_return(result);
};
SCM scm_dg_exists_p(SCM scm_txn, SCM scm_ids) {
  status_init;
  dg_ids_t *ids = 0;
  status_require_x(scm_to_dg_ids(scm_ids, &ids));
  boolean result_c;
  status_require_x(dg_exists_p(scm_to_txn(scm_txn), ids, &result_c));
exit:
  dg_ids_destroy(ids);
  status_to_scm_return(scm_from_bool(result_c));
};
SCM scm_dg_intern_ensure(SCM scm_txn, SCM scm_data) {
  status_init;
  dg_data_list_t *data = 0;
  status_require_x(scm_to_dg_data_list(scm_data, &data));
  dg_ids_t *ids = 0;
  status_require_x(dg_intern_ensure(scm_to_txn(scm_txn), data, &ids));
  SCM result = dg_ids_to_scm(ids);
exit:
  dg_ids_destroy(ids);
  dg_data_list_data_free(data);
  dg_data_list_destroy(data);
  status_to_scm_return(result);
};
SCM scm_dg_intern_update(SCM scm_txn, SCM scm_id, SCM scm_data) {
  status_init;
  dg_data_t data = {0, 0};
  status_require_x(scm_to_dg_data(scm_data, &data));
  dg_id_t id = scm_to_dg_id(scm_id);
  status_require_x(dg_intern_update(scm_to_txn(scm_txn), id, data));
exit:
  free(data.data);
  status_to_scm_return(SCM_BOOL_T);
};
SCM scm_dg_extern_update(SCM scm_txn, SCM scm_id, SCM scm_data) {
  status_init;
  dg_data_t data;
  status_require_x(scm_to_dg_data(scm_data, &data));
  dg_id_t id = scm_to_dg_id(scm_id);
  status_require_x(dg_extern_update(scm_to_txn(scm_txn), id, data));
exit:
  free(data.data);
  status_to_scm_return(SCM_BOOL_T);
};
SCM scm_dg_status_description(SCM id_status, SCM id_group) {
  status_init;
  status.id = scm_to_int(id_status);
  status.group = scm_to_int(id_group);
  scm_from_latin1_string(dg_status_description(status));
};
SCM scm_dg_status_group_id_to_name(SCM a) {
  scm_from_latin1_symbol(dg_status_group_id_to_name(scm_to_int(a)));
};
SCM scm_dg_intern_data_to_id(SCM scm_txn, SCM scm_data, SCM scm_every_p) {
  status_init;
  boolean every_p = optional_every_p(scm_every_p);
  dg_data_list_t *data = 0;
  status_require_x(scm_to_dg_data_list(scm_data, &data));
  dg_ids_t *ids = 0;
  status = dg_intern_data_to_id(scm_to_txn(scm_txn), data, every_p, &ids);
  if ((dg_status_id_condition_unfulfilled == status.id)) {
    status_reset;
  } else {
    status_require;
  };
  SCM result = dg_ids_to_scm(ids);
exit:
  dg_ids_destroy(ids);
  dg_data_list_data_free(data);
  dg_data_list_destroy(data);
  status_to_scm_return(result);
};
SCM scm_dg_intern_id_to_data(SCM scm_txn, SCM scm_ids, SCM scm_every_p) {
  status_init;
  boolean every_p = optional_every_p(scm_every_p);
  dg_ids_t *ids = 0;
  status_require_x(scm_to_dg_ids(scm_ids, &ids));
  dg_data_list_t *data = 0;
  status = dg_intern_id_to_data(scm_to_txn(scm_txn), ids, every_p, &data);
  if ((dg_status_id_condition_unfulfilled == status.id)) {
    status_reset;
  } else {
    status_require;
  };
  SCM result = dg_data_list_to_scm(data);
exit:
  dg_ids_destroy(ids);
  dg_data_list_destroy(data);
  status_to_scm_return(result);
};
SCM scm_dg_intern_small_p(SCM id_scm) {
  scm_from_bool(dg_intern_small_p(scm_to_dg_id(id_scm)));
};
SCM scm_dg_intern_small_id_to_data(SCM id_scm) {
  dg_id_to_scm(dg_intern_small_id_to_data(scm_to_dg_id(id_scm)));
};
SCM scm_dg_intern_small_data_to_id(SCM data_scm) {
  dg_id_to_scm(dg_intern_small_data_to_id(scm_to_dg_id(data_scm)));
};
#define result_add(key, struct_key)                                            \
  scm_c_alist_add_from_struct(result, result_c, key, struct_key, dg_ids_to_scm)
#define result_add_records(key, struct_key)                                    \
  scm_c_alist_add_from_struct(result, result_c, key, struct_key,               \
                              dg_relation_records_to_scm)
SCM scm_dg_index_errors_intern(SCM scm_txn) {
  status_init;
  dg_index_errors_intern_t result_c;
  status_require_x(dg_index_errors_intern(scm_to_txn(scm_txn), &result_c));
  SCM result = SCM_EOL;
  result_add("different-data-id", different_data_id);
  result_add("excess-data-id", excess_data_id);
  result_add("different-id-data", different_id_data);
  result_add("missing-id-data", missing_id_data);
  scm_c_alist_add_from_struct(result, result_c, "errors?", errors_p,
                              scm_from_bool);
exit:
  status_to_scm_return(result);
};
SCM scm_dg_index_errors_extern(SCM scm_txn) {
  status_init;
  dg_index_errors_extern_t result_c;
  status_require_x(dg_index_errors_extern(scm_to_txn(scm_txn), &result_c));
  SCM result = SCM_EOL;
  result_add("different-data-extern", different_data_extern);
  result_add("excess-data-extern", excess_data_extern);
  result_add("different-id-data", different_id_data);
  result_add("missing-id-data", missing_id_data);
  scm_c_alist_add_from_struct(result, result_c, "errors?", errors_p,
                              scm_from_bool);
exit:
  status_to_scm_return(result);
};
SCM scm_dg_index_errors_relation(SCM scm_txn) {
  status_init;
  dg_index_errors_relation_t result_c;
  status_require_x(dg_index_errors_relation(scm_to_txn(scm_txn), &result_c));
  SCM result = SCM_EOL;
  result_add_records("missing-right-left", missing_right_left);
  result_add_records("missing-label-left", missing_label_left);
  result_add_records("excess-right-left", excess_right_left);
  result_add_records("excess-label-left", excess_label_left);
  scm_c_alist_add_from_struct(result, result_c, "errors?", errors_p,
                              scm_from_bool);
exit:
  status_to_scm_return(result);
};
#undef result_add
#undef result_add_records
#define define_scm_dg_index_recreate(name)                                     \
  SCM scm_dg_index_recreate_##name() {                                         \
    status_init;                                                               \
    status_require_x(dg_index_recreate_##name());                              \
  exit:                                                                        \
    status_to_scm_return(SCM_BOOL_T);                                          \
  }
define_scm_dg_index_recreate(intern);
define_scm_dg_index_recreate(extern);
define_scm_dg_index_recreate(relation);
SCM scm_dg_node_select(SCM scm_txn, SCM scm_types, SCM scm_offset) {
  if (scm_is_null(scm_types)) {
    return (selection_to_scm(0));
  };
  status_init;
  b32 offset = optional_offset(scm_offset);
  dg_node_read_state_t *state = malloc(sizeof(dg_node_read_state_t));
  if (!state) {
    status_set_id_goto(dg_status_id_memory);
  };
  b8 types = optional_types(scm_types);
  status_require_x(dg_node_select(scm_to_txn(scm_txn), types, offset, state));
exit:
  if ((status_failure_p && !status_id_is_p(dg_status_id_no_more_data))) {
    free(state);
    return (status_to_scm_error(status));
  };
  active_read_states_add_x(state, dg_read_state_type_node);
  return (selection_to_scm(state));
};
SCM scm_dg_node_read(SCM scm_selection, SCM scm_count) {
  status_init;
  dg_node_read_state_t *state = scm_to_selection(scm_selection, node);
  b32 count = optional_count(scm_count);
  dg_data_records_t *records = 0;
  dg_status_require_read_x(dg_node_read(state, count, &records));
  dg_status_success_if_no_more_data;
  SCM result = dg_data_records_to_scm(records, dg_data_record_to_scm);
exit:
  status.group = dg_status_group_lmdb;
  dg_data_records_destroy(records);
  status_to_scm_return(result);
};
#define dg_status_require_malloc(malloc_result)                                \
  if (!malloc_result) {                                                        \
    status_set_both_goto(dg_status_group_dg, dg_status_id_memory);             \
  }
#define set_ordinal_match_data(scm_ordinal)                                    \
  dg_ordinal_match_data_t *ordinal = 0;                                        \
  if (scm_is_true(scm_list_p(scm_ordinal))) {                                  \
    SCM scm_ordinal_min =                                                      \
        scm_assoc_ref(scm_ordinal, scm_from_latin1_symbol("min"));             \
    SCM scm_ordinal_max =                                                      \
        scm_assoc_ref(scm_ordinal, scm_from_latin1_symbol("max"));             \
    ordinal = calloc(1, sizeof(dg_ordinal_match_data_t));                      \
    dg_status_require_malloc(ordinal);                                         \
    (*ordinal).min =                                                           \
        (scm_is_integer(scm_ordinal_min) ? scm_to_uint(scm_ordinal_min) : 0);  \
    (*ordinal).max =                                                           \
        (scm_is_integer(scm_ordinal_max) ? scm_to_uint(scm_ordinal_max) : 0);  \
  }
SCM scm_dg_relation_select(SCM scm_txn, SCM scm_left, SCM scm_right,
                           SCM scm_label, SCM scm_retrieve, SCM scm_ordinal,
                           SCM scm_offset) {
  if ((scm_is_null(scm_left) || scm_is_null(scm_right) ||
       scm_is_null(scm_label))) {
    return (selection_to_scm(0));
  };
  status_init;
  set_ordinal_match_data(scm_ordinal);
  b32 offset = optional_offset(scm_offset);
  dg_guile_relation_read_state_t *state =
      malloc(sizeof(dg_guile_relation_read_state_t));
  dg_status_require_malloc(state);
  dg_define_ids_3(left, right, label);
  optional_ids(scm_left, left);
  optional_ids(scm_right, right);
  optional_ids(scm_label, label);
  status_require_x(dg_relation_select(scm_to_txn(scm_txn), left, right, label,
                                      ordinal, offset, &(*state).dg_state));
  SCM (*records_to_scm)
  (dg_relation_records_t *) = optional_relation_retrieve(scm_retrieve);
  if (!records_to_scm) {
    status_set_both_goto(dg_status_group_dg, dg_status_id_input_type);
  };
  (*state).left = left;
  (*state).right = right;
  (*state).label = label;
  (*state).records_to_scm = records_to_scm;
exit:
  if (status_failure_p) {
    free(state);
    free(left);
    free(right);
    free(label);
    if (status_id_is_p(dg_status_id_no_more_data)) {
      return (selection_to_scm(0));
    } else {
      status_to_scm_error(status);
    };
  } else {
    active_read_states_add_x(state, dg_read_state_type_relation);
    return (selection_to_scm(state));
  };
};
SCM scm_dg_relation_delete(SCM scm_txn, SCM scm_left, SCM scm_right,
                           SCM scm_label, SCM scm_ordinal) {
  if ((scm_is_null(scm_left) || scm_is_null(scm_right) ||
       scm_is_null(scm_label))) {
    return (SCM_BOOL_T);
  };
  status_init;
  set_ordinal_match_data(scm_ordinal);
  dg_define_ids_3(left, right, label);
  optional_ids(scm_left, left);
  optional_ids(scm_right, right);
  optional_ids(scm_label, label);
  status_require_x(
      dg_relation_delete(scm_to_txn(scm_txn), left, right, label, ordinal));
exit:
  status_to_scm_return(SCM_BOOL_T);
};
SCM scm_dg_relation_read(SCM scm_selection, SCM scm_count) {
  status_init;
  dg_guile_relation_read_state_t *state =
      scm_to_selection(scm_selection, guile_relation);
  if (!state) {
    return (SCM_EOL);
  };
  b32 count = optional_count(scm_count);
  dg_relation_records_t *records = 0;
  SCM (*records_to_scm)(dg_relation_records_t *) = (*state).records_to_scm;
  dg_status_require_read_x(
      dg_relation_read(&(*state).dg_state, count, &records));
  dg_status_success_if_no_more_data;
  SCM result = records_to_scm(records);
exit:
  dg_relation_records_destroy(records);
  status_to_scm_return(result);
};
SCM scm_dg_txn_p(SCM a) {
  return (scm_from_bool(SCM_SMOB_PREDICATE(scm_type_txn, a)));
};
SCM scm_dg_txn_active_p(SCM a) { return (scm_from_bool(SCM_SMOB_DATA(a))); };
SCM scm_dg_selection_p(SCM a) {
  return (scm_from_bool(SCM_SMOB_PREDICATE(scm_type_selection, a)));
};
SCM scm_dg_debug_count_all_btree_entries(SCM txn) {
  status_init;
  b32 result;
  status_require_x(dg_debug_count_all_btree_entries(scm_to_txn(txn), &result));
exit:
  status_to_scm_return(scm_from_uint32(result));
};
SCM scm_dg_debug_display_btree_counts(SCM txn) {
  status_init;
  status_require_x(dg_debug_display_btree_counts(scm_to_txn(txn)));
exit:
  status_to_scm_return(SCM_BOOL_T);
};
SCM scm_dg_debug_display_content_left_to_right(SCM txn) {
  status_init;
  dg_status_require_read_x(
      dg_debug_display_content_left_to_right(scm_to_txn(txn)));
exit:
  status_to_scm_return(SCM_BOOL_T);
};
SCM scm_dg_debug_display_content_right_to_left(SCM txn) {
  status_init;
  dg_status_require_read_x(
      dg_debug_display_content_right_to_left(scm_to_txn(txn)));
exit:
  status_to_scm_return(SCM_BOOL_T);
};
b0 dg_guile_init() {
  scm_type_txn = scm_make_smob_type("dg-txn", 1);
  scm_type_selection = scm_make_smob_type("dg-selection", 0);
  dg_scm_write = scm_variable_ref(scm_c_lookup("write"));
  dg_scm_read = scm_variable_ref(scm_c_lookup("read"));
  scm_symbol_label = scm_from_latin1_symbol("label");
  scm_symbol_ordinal = scm_from_latin1_symbol("ordinal");
  scm_symbol_left = scm_from_latin1_symbol("left");
  scm_symbol_right = scm_from_latin1_symbol("right");
  scm_rnrs_raise = scm_c_public_ref("rnrs exceptions", "raise");
  scm_bytevector_null = scm_c_make_bytevector(0);
  SCM m = scm_c_resolve_module("sph storage dg");
  scm_c_module_define(m, "dg-init-extension", SCM_EOL);
  scm_c_module_define(m, "dg-size-octets-id",
                      scm_from_size_t(dg_size_octets_id));
  scm_c_module_define(
      m, "dg-size-octets-data-max",
      scm_from_size_t((dg_size_octets_data_max - dg_guile_intern_type_size)));
  scm_c_module_define(m, "dg-size-octets-data-min",
                      scm_from_size_t(dg_size_octets_data_min));
  scm_c_module_define(m, "dg-null", scm_from_uint8(dg_null));
  scm_c_module_define(m, "dg-type-bit-id", scm_from_uint8(dg_type_bit_id));
  scm_c_module_define(m, "dg-type-bit-intern",
                      scm_from_uint8(dg_type_bit_intern));
  scm_c_module_define(m, "dg-type-bit-extern",
                      scm_from_uint8(dg_type_bit_extern));
  scm_c_module_define(m, "dg-type-bit-intern-small",
                      scm_from_uint8(dg_type_bit_intern_small));
  scm_c_define_procedure_c_init;
  scm_c_define_procedure_c("dg-exit", 0, 0, 0, scm_dg_exit,
                           "completely deinitialises the database");
  scm_c_define_procedure_c("dg-init", 1, 1, 0, scm_dg_init,
                           "path [options] ->");
  scm_c_define_procedure_c("dg-id?", 1, 0, 0, scm_dg_id_p,
                           "integer -> boolean");
  scm_c_define_procedure_c("dg-intern?", 1, 0, 0, scm_dg_intern_p,
                           "integer -> boolean");
  scm_c_define_procedure_c("dg-extern?", 1, 0, 0, scm_dg_extern_p,
                           "integer -> boolean");
  scm_c_define_procedure_c("dg-relation?", 1, 0, 0, scm_dg_relation_p,
                           "integer -> boolean");
  scm_c_define_procedure_c("dg-initialised?", 0, 0, 0, scm_dg_initialised_p,
                           "-> boolean");
  scm_c_define_procedure_c("dg-root", 0, 0, 0, scm_dg_root, "-> string");
  scm_c_define_procedure_c("dg-txn-create-read", 0, 0, 0,
                           scm_dg_txn_create_read, "-> dg-txn");
  scm_c_define_procedure_c("dg-txn-create-write", 0, 0, 0,
                           scm_dg_txn_create_write, "-> dg-txn");
  scm_c_define_procedure_c("dg-txn-abort", 1, 0, 0, scm_dg_txn_abort,
                           "dg-txn ->");
  scm_c_define_procedure_c("dg-txn-commit", 1, 0, 0, scm_dg_txn_commit,
                           "dg-txn -> unspecified");
  scm_c_define_procedure_c("dg-id-create", 1, 1, 0, scm_dg_id_create,
                           "dg-txn [count] -> (integer ...)");
  scm_c_define_procedure_c("dg-identify", 2, 0, 0, scm_dg_identify,
                           "dg-txn (integer:id ...) -> list");
  scm_c_define_procedure_c("dg-exists?", 2, 0, 0, scm_dg_exists_p,
                           "dg-txn (integer:id ...) -> list");
  scm_c_define_procedure_c("dg-statistics", 1, 0, 0, scm_dg_statistics,
                           "dg-txn -> alist");
  scm_c_define_procedure_c(
      "dg-relation-ensure", 3, 3, 0, scm_dg_relation_ensure,
      "dg-txn list list [list false/procedure integer/any] -> list:ids");
  scm_c_define_procedure_c("dg-intern-ensure", 2, 0, 0, scm_dg_intern_ensure,
                           "dg-txn list -> list:ids");
  scm_c_define_procedure_c("dg-status-description", 2, 0, 0,
                           scm_dg_status_description,
                           "integer:status integer:group -> string");
  scm_c_define_procedure_c("dg-status-group-id->name", 1, 0, 0,
                           scm_dg_status_group_id_to_name,
                           "integer:group-id -> string");
  scm_c_define_procedure_c("dg-intern-id->data", 2, 2, 0,
                           scm_dg_intern_id_to_data,
                           "dg-txn list [boolean:every?] -> (any ...)");
  scm_c_define_procedure_c("dg-intern-data->id", 2, 1, 0,
                           scm_dg_intern_data_to_id,
                           "dg-txn list [boolean:every?] -> (integer ...)");
  scm_c_define_procedure_c("dg-intern-small?", 1, 0, 0, scm_dg_intern_small_p,
                           "id -> boolean");
  scm_c_define_procedure_c("dg-intern-small-data->id", 1, 0, 0,
                           scm_dg_intern_small_data_to_id, "integer -> id");
  scm_c_define_procedure_c("dg-intern-small-id->data", 1, 0, 0,
                           scm_dg_intern_small_id_to_data, "id -> integer");
  scm_c_define_procedure_c("dg-delete", 2, 0, 0, scm_dg_delete,
                           "dg-txn list -> unspecified");
  scm_c_define_procedure_c("dg-extern-create", 1, 2, 0, scm_dg_extern_create,
                           "dg-txn [integer:count any:data] -> list");
  scm_c_define_procedure_c("dg-extern-id->data", 2, 2, 0,
                           scm_dg_extern_id_to_data,
                           "dg-txn (integer ...) [boolean:every?] -> list");
  scm_c_define_procedure_c("dg-extern-data->id", 2, 0, 0,
                           scm_dg_extern_data_to_id, "dg-txn any -> list");
  scm_c_define_procedure_c("dg-index-errors-relation", 1, 0, 0,
                           scm_dg_index_errors_relation, "dg-txn -> list");
  scm_c_define_procedure_c("dg-index-errors-intern", 1, 0, 0,
                           scm_dg_index_errors_intern, "dg-txn -> list");
  scm_c_define_procedure_c("dg-index-errors-extern", 1, 0, 0,
                           scm_dg_index_errors_extern, "dg-txn -> list");
  scm_c_define_procedure_c("dg-index-recreate-intern", 0, 0, 0,
                           scm_dg_index_recreate_intern, "-> true");
  scm_c_define_procedure_c("dg-index-recreate-extern", 0, 0, 0,
                           scm_dg_index_recreate_extern, "-> true");
  scm_c_define_procedure_c("dg-index-recreate-relation", 0, 0, 0,
                           scm_dg_index_recreate_relation, "-> true");
  scm_c_define_procedure_c("dg-node-select", 1, 2, 0, scm_dg_node_select,
                           "dg-txn [types offset] -> dg-selection\n    types "
                           "is zero or a combination of bits from "
                           "dg-type-bit-* variables, for example (logior "
                           "dg-type-bit-intern dg-type-bit-extern)");
  scm_c_define_procedure_c("dg-node-read", 1, 1, 0, scm_dg_node_read,
                           "dg-selection [count] -> (vector ...)");
  scm_c_define_procedure_c(
      "dg-relation-select", 1, 6, 0, scm_dg_relation_select,
      "dg-txn (integer ...):left [(integer ...):right (integer ...):label "
      "symbol:retrieve-only-field list:((symbol:min integer) (symbol:max "
      "integer)):ordinal integer:offset] -> dg-selection");
  scm_c_define_procedure_c("dg-relation-delete", 2, 3, 0,
                           scm_dg_relation_delete,
                           "dg-txn (integer ...):left [(integer ...):right "
                           "(integer ...):label list:((symbol:min integer) "
                           "(symbol:max integer)):ordinal] -> unspecified");
  scm_c_define_procedure_c("dg-relation-read", 1, 1, 0, scm_dg_relation_read,
                           "dg-selection [integer:count] -> (vector ...)");
  scm_c_define_procedure_c("dg-intern-update", 3, 0, 0, scm_dg_intern_update,
                           "dg-txn integer:id any:data -> true");
  scm_c_define_procedure_c("dg-extern-update", 3, 0, 0, scm_dg_extern_update,
                           "dg-txn integer:id any:data -> true");
  scm_c_define_procedure_c("dg-selection?", 1, 0, 0, scm_dg_selection_p,
                           "any -> boolean");
  scm_c_define_procedure_c("dg-txn?", 1, 0, 0, scm_dg_txn_p, "any -> boolean");
  scm_c_define_procedure_c("dg-txn-active?", 1, 0, 0, scm_dg_txn_active_p,
                           "dg-txn -> boolean");
  scm_c_define_procedure_c("dg-debug-count-all-btree-entries", 1, 0, 0,
                           scm_dg_debug_count_all_btree_entries,
                           "dg-txn -> integer");
  scm_c_define_procedure_c("dg-debug-display-btree-counts", 1, 0, 0,
                           scm_dg_debug_display_btree_counts, "dg-txn ->");
  scm_c_define_procedure_c("dg-debug-display-content-left->right", 1, 0, 0,
                           scm_dg_debug_display_content_left_to_right,
                           "dg-txn ->");
  scm_c_define_procedure_c("dg-debug-display-content-right->left", 1, 0, 0,
                           scm_dg_debug_display_content_right_to_left,
                           "dg-txn ->");
};