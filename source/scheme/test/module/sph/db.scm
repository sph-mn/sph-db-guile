(define-test-module (test module sph db)
  (import
    (sph db)
    (sph list)
    (test helper sph db))

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

  (define-test (db-type env)
    (let*
      ( (type-name "test-type")
        (fields (q (("field-1" . int64) ("field-2" . uint8) ("field-3" . string))))
        (type (db-type-create env type-name fields)))
      (assert-and (equal? 1 (db-type-id type)) (equal? type-name (db-type-name type))
        (equal? 0 (db-type-flags type)) (equal? fields (db-type-fields type))
        (null? (db-type-indices type)) (not (not (db-type-get env "test-type")))
        (begin (db-type-delete type) (not (db-type-get env "test-type"))))))

  (define-procedure-tests tests (db-type) (db-env) (db-txn) (db-statistics))

  (l (settings)
    (let* ((test-runs 1) (settings (test-helper-db-default-test-settings settings)))
      (test-helper-delete-database-files)
      (apply append (map-integers test-runs (l (n) (test-execute-procedures settings tests)))))))
