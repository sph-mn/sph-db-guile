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
      (assert-and "basics" (db-env-open? env)
        (string? (db-env-root env)) (integer? (db-env-format env)) (integer? (db-env-maxkeysize env)))
      (assert-equal "root" (db-env-root env) test-helper-db-database-root)))

  (define-test (db-txn env)
    (let
      (test-combination
        (l (txn-begin txn-end)
          (let (txn (txn-begin env))
            (and (db-txn-active? txn) (begin (txn-end txn) #t) (not (db-txn-active? txn))))))
      (assert-and (assert-true "begin-abort" (test-combination db-txn-begin db-txn-abort))
        (assert-true "begin-commit" (test-combination db-txn-begin db-txn-commit))
        (assert-true "write-begin-abort" (test-combination db-txn-write-begin db-txn-abort))
        (assert-true "write-begin-commit" (test-combination db-txn-write-begin db-txn-commit)))))

  (define-test (db-statistics env)
    (db-txn-call-read env (l (txn) (let (a (db-statistics txn)) (and (list? a) (every pair? a))))))

  (define-procedure-tests tests (db-env) (db-txn) (db-statistics))

  (l (settings)
    (let* ((test-runs 1) (settings (test-helper-db-default-test-settings settings)))
      (test-helper-delete-database-files)
      (apply append (map-integers test-runs (l (n) (test-execute-procedures settings tests)))))))
