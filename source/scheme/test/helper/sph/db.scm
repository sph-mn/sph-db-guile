(library (test helper sph db)
  (export)
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

  (define test-helper-db-database-path-root "/tmp/test/sph-db")

  (define (test-helper-db-database-open)
    (set! test-helper-db-database-path (test-helper-db-database-path-next))
    (db-init test-helper-db-database-path))

  (define (test-helper-db-database-delete))

  (define (test-helper-db-default-test-settings settings)
    "add/append the default settings to the given test settings object.
     by default, the database is created before each procedure test.
     after each procedure test the database is deleted"
    (let
      ( (procedure-wrap
          (l (test-proc test-name) (if (db-env-open env) (db-close env))
            (db-use test-helper-db-database-root (l (env)))
            (if (file-exists? test-helper-db-database-path-root)
              (system* "rm" "-r" test-helper-db-database-path-root))))
        (existing-procedure-wrap (alist-ref settings (q procedure-wrap))))
      (alist-set settings (q procedure-wrap)
        (if existing-procedure-wrap
          (procedure-append-ignore-result existing-procedure-wrap procedure-wrap))))))
