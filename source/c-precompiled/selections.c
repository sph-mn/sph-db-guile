/* generic db-selection type with the option to carry data to be freed when the
  selection isnt needed anymore. db-guile-selection-type-t is the list element
  type */
#define db_guile_selections_first mi_list_first
#define db_guile_selections_rest mi_list_rest
#define mi_list_name_prefix db_guile_selections
#define mi_list_element_t db_guile_selection_t
typedef enum {
  db_guile_selection_type_relation,
  db_guile_selection_type_record
} db_guile_selection_type_t;
typedef struct {
  void* selection;
  db_guile_selection_type_t selection_type;
} db_guile_selection_t;
typedef struct {
  SCM matcher;
  status_id_t status_id;
  db_record_selection_t selection;
} db_guile_record_selection_t;
typedef struct {
  db_ids_t left;
  db_ids_t right;
  db_ids_t label;
  status_id_t status_id;
  SCM (*scm_from_relations)(db_relations_t);
  db_relation_selection_t selection;
} db_guile_relation_selection_t;
typedef struct {
  status_id_t status_id;
  db_index_selection_t selection;
} db_guile_index_selection_t;
typedef struct {
  status_id_t status_id;
  db_record_index_selection_t selection;
} db_guile_record_index_selection_t;
#include "./foreign/sph/mi-list.c"
__thread db_guile_selections_t* db_guile_active_selections = 0;
/** finish all selections and associated temporary data of the current thread.
  so that no call to selection-finish is necessary in scheme.
  called by txn-commit or txn-abort.
  there can only be one active transaction per thread per sph-db requirements */
void db_guile_selections_free() {
  db_guile_selection_t a;
  db_guile_relation_selection_t relation_selection;
  db_guile_record_selection_t record_selection;
  while (db_guile_active_selections) {
    a = db_guile_selections_first(db_guile_active_selections);
    if (db_guile_selection_type_relation == a.selection_type) {
      relation_selection = *((db_guile_relation_selection_t*)(a.selection));
      db_relation_selection_finish((&(relation_selection.selection)));
      db_ids_free((relation_selection.left));
      db_ids_free((relation_selection.label));
      db_ids_free((relation_selection.right));
      free((a.selection));
    } else if (db_guile_selection_type_record == a.selection_type) {
      record_selection = *((db_guile_record_selection_t*)(a.selection));
      db_record_selection_finish((&(record_selection.selection)));
      free((a.selection));
    };
    db_guile_active_selections =
      db_guile_selections_drop(db_guile_active_selections);
  };
};
/** add a new db-guile-selection object to db-guile-active-selections */
void db_guile_selection_register(void* db_selection,
  db_guile_selection_type_t selection_type) {
  status_declare;
  db_guile_selections_t* a;
  db_guile_selection_t b;
  /* add db-guile-selection to linked-list */
  b.selection = db_selection;
  b.selection_type = selection_type;
  a = db_guile_selections_add(db_guile_active_selections, b);
  if (a) {
    db_guile_active_selections = a;
  } else {
    status_set_both(db_status_group_db, db_status_id_memory);
    scm_from_status_error(status);
  };
};