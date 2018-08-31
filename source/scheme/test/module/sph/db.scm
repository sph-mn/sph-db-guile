(define-test-module (test module sph db)
  (import
    (sph db)
    (test helper sph db)
    (sph common)
    (sph char-set-vector)
    (ice-9 threads)
    (rnrs bytevectors)
    (sph random-data)
    (srfi srfi-41)
    (sph stream)
    (sph vector selection))

  (define-test (db-env env)
    (assert-and
      (assert-and "basics" (db-env? env)
        (not (db-env? #f)) (db-env-open? env)
        (string? (db-env-root env)) (integer? (db-env-format env)) (integer? (db-env-maxkeysize env)))
      (assert-equal "root" (db-env-root env) test-helper-db-database-root)))

  (define-procedure-tests tests (db-env))

  (l (settings)
    (let* ((test-runs 1) (settings (test-helper-db-default-test-settings settings)))
      (test-helper-delete-database-files)
      (apply append (map-integers test-runs (l (n) (test-execute-procedures settings tests)))))))
