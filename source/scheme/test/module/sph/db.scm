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

  (l (settings)
    (let*
      ( (test-runs 1)
        (settings
          (alist-set-multiple-q (test-helper-db-default-test-settings settings) exception->key #t))
        (result
          (apply append (map-integers test-runs (l (n) (test-execute-procedures settings tests))))))
      (test-helper-db-database-close) result)))
