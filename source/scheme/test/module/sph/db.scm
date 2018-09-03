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
    (test-helper-type-create-1 env
      (l (name fields type)
        (assert-and (equal? 1 (db-type-id type)) (equal? name (db-type-name type))
          (equal? 0 (db-type-flags type)) (equal? fields (db-type-fields type))
          (null? (db-type-indices type)) (not (not (db-type-get env "test-type")))
          (begin (db-type-delete type) (not (db-type-get env "test-type")))))))

  (define-test (db-index env)
    (test-helper-type-create-1 env
      (l (type-name type-fields type)
        (let* ((index-fields (list 1 "field-3")) (index (db-index-create type index-fields)))
          (assert-and (not (not index))
            (equal? (db-index-fields index) (db-index-fields (db-index-get type index-fields)))
            (begin (db-index-rebuild index) #t) (begin (db-index-delete index) #t)
            (not (db-index-get type index-fields)))))))

  (define-procedure-tests tests (db-index) (db-type) (db-env) (db-txn) (db-statistics))

  (l (settings)
    (let* ((test-runs 1) (settings (test-helper-db-default-test-settings settings)))
      (test-helper-delete-database-files)
      (apply append (map-integers test-runs (l (n) (test-execute-procedures settings tests)))))))
