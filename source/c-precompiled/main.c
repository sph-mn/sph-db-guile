/* sph-db-guile basically registers scheme procedures that when called execute
 * specific c-functions that manage calls to sph-db */
#include <libguile.h>
#include <sph-db.h>
#include "./foreign/sph/one.c"
#include "./foreign/sph/guile.c"
#include "./helper.c"
SCM scm_db_env_p(SCM a) {
  return ((scm_from_bool((SCM_SMOB_PREDICATE(scm_type_env, a)))));
};
SCM scm_db_txn_p(SCM a) {
  return ((scm_from_bool((SCM_SMOB_PREDICATE(scm_type_txn, a)))));
};
SCM scm_db_env_open_p(SCM a) {
  return ((scm_from_bool(((scm_to_db_env(a))->is_open))));
};
SCM scm_db_env_root(SCM a) {
  return ((scm_from_locale_string(((scm_to_db_env(a))->root))));
};
SCM scm_db_env_maxkeysize(SCM a) {
  return ((scm_from_uint32(((scm_to_db_env(a))->maxkeysize))));
};
SCM scm_db_env_format(SCM a) {
  return ((scm_from_uint32(((scm_to_db_env(a))->format))));
};
SCM scm_db_selection_p(SCM a) {
  return ((scm_from_bool((SCM_SMOB_PREDICATE(scm_type_selection, a)))));
};
SCM scm_db_open(SCM scm_root, SCM scm_options) {
  status_declare;
  db_env_declare(env);
  db_open_options_t options;
  db_open_options_t* options_pointer;
  SCM a;
  uint8_t* root;
  root = 0;
  root = scm_to_locale_string(scm_root);
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
/** prepare scm valuaes and register guile bindings */
void db_guile_init() {
  scm_type_txn = scm_make_smob_type("db-txn", 0);
  scm_type_env = scm_make_smob_type("db-env", 0);
  scm_type_selection = scm_make_smob_type("db-selection", 0);
  scm_rnrs_raise = scm_c_public_ref("rnrs exceptions", "raise");
  scm_c_define_procedure_c_init;
  scm_c_define_procedure_c("db-open",
    1,
    1,
    0,
    scm_db_open,
    ("string:root [((key . value) ...):options] ->"));
  scm_c_define_procedure_c(
    "db-close", 1, 0, 0, scm_db_close, "deinitialises the database handle");
  scm_c_define_procedure_c("db-env?", 1, 0, 0, scm_db_env_p, "");
  scm_c_define_procedure_c("db-txn?", 1, 0, 0, scm_db_txn_p, "");
  scm_c_define_procedure_c("db-selection?", 1, 0, 0, scm_db_selection_p, "");
  scm_c_define_procedure_c("db-env-open?", 1, 0, 0, scm_db_env_open_p, "");
  scm_c_define_procedure_c(
    "db-env-maxkeysize", 1, 0, 0, scm_db_env_maxkeysize, "");
  scm_c_define_procedure_c("db-env-root", 1, 0, 0, scm_db_env_root, "");
  scm_c_define_procedure_c("db-env-format", 1, 0, 0, scm_db_env_format, "");
};