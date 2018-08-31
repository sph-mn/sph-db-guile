(library (test helper sph db)
  (export
    test-helper-db-database-root
    test-helper-db-default-test-settings
    test-helper-delete-database-files)
  (import
    (guile)
    (rnrs bytevectors)
    (sph)
    (sph alist)
    (sph char-set-vector)
    (sph db)
    (sph one)
    (sph random-data)
    (only (rnrs base) set!)
    (only (sph list) map-integers))

  (define test-helper-db-database-root "/tmp/test/sph-db")
  (define (delete-file-if-exists a) (and (file-exists? a) (delete-file a)))

  (define (test-helper-delete-database-files)
    (each delete-file-if-exists
      (list (string-append test-helper-db-database-root "/data")
        (string-append test-helper-db-database-root "/data-lock"))))

  (define (test-helper-db-default-test-settings settings)
    "add/append the default settings to the given test settings object.
     by default, the database is created before each procedure test.
     after each procedure test the database is deleted"
    (let
      ( (procedure-wrap
          (l (test-proc test-name)
            (l a
              (begin-first (db-use test-helper-db-database-root (l (env) (apply test-proc env a)))
                (test-helper-delete-database-files)))))
        (existing-procedure-wrap (alist-ref settings (q procedure-wrap))))
      (alist-set-multiple settings (q exception->key)
        #t (q procedure-wrap)
        (if existing-procedure-wrap
          (procedure-append-ignore-result existing-procedure-wrap procedure-wrap) procedure-wrap)))))
