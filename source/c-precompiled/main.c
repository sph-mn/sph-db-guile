/* sph-db-guile basically registers scheme procedures that when called execute
 * specific c-functions that manage calls to sph-db */
#include <libguile.h>
#include <sph-db.h>
#include "./foreign/sph/one.c"
#include "./forein/sph/guile.c"
#include "./helper.c"
SCM scm_db_env_p(SCM a) {
  return ((scm_from_bool((SCM_SMOB_PREDICATE(scm_type_env, a)))));
};
SCM scm_db_txn_p(SCM a) {
  return ((scm_from_bool((SCM_SMOB_PREDICATE(scm_type_txn, a)))));
};
SCM scm_db_selection_p(SCM a) {
  return ((scm_from_bool((SCM_SMOB_PREDICATE(scm_type_selection, a)))));
};
SCM scm_db_open(SCM scm_root, SCM scm_options) {
  status_declare;
  db_env_declare(env);
  db_open_options_t options;
  db_open_options_t* options_pointer;
  SCM scm_temp;
  uint8_t root;
  root = 0;
  root = scm_to_locale_string(scm_root);
  status_require((db_env_new((&env))));
  if (scm_is_undefined(scm_options) || scm_is_null(scm_options)) {
    options_pointer = 0;
  } else {
    db_open_options_set_defaults((&options));
    scm_options_get(scm_options, "read-only?", scm_temp);
    if (scm_is_bool(scm_temp)) {
      options.read_only_p = scm_is_true(scm_temp);
    };
    scm_options_get("maximum-size-octets");
    if (scm_is_integer(scm_temp)) {
      options.maximum_size_octets = scm_to_uint(scm_temp);
    };
    scm_options_get("maximum-reader-count");
    if (scm_is_integer(scm_temp)) {
      options.maximum_reader_count = scm_to_uint(scm_temp);
    };
    scm_options_get("filesystem-has-ordered-writes?");
    if (scm_is_bool(scm_temp)) {
      options.filesystem_has_ordered_writes_p = scm_is_true(scm_temp);
    };
    scm_options_get("env-open-flags");
    if (scm_is_integer(scm_temp)) {
      options.env_open_flags = scm_to_uint(scm_temp);
    };
    scm_options_get("file-permissions");
    if (scm_is_integer(scm_temp)) {
      options.file_permissions = scm_to_uint(scm_temp);
    };
    options_pointer = &options;
  };
  status = db_open(root, options_pointer, (&env));
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
/** map and register guile bindings */
b0 db_guile_init() {
  scm_type_txn = scm_make_smob_type("db-txn", 0);
  scm_type_env = scm_make_smob_type("db-env", 0);
  scm_type_selection = scm_make_smob_type("db-selection", 0);
  scm_c_define_procedure_c("db-open",
    1,
    1,
    0,
    scm_db_open,
    ("string:root [((key . value) ...):options] ->"));
  scm_c_define_procedure_c(
    "db-close", 1, 0, 0, scm_db_exit, "deinitialises the database handle");
};