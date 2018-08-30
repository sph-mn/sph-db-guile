(library (test helper sph db)
  (export
    test-helper-db-add-default-test-settings
    test-helper-db-count-btree-entries
    test-helper-db-create-relations
    test-helper-db-data-linear
    test-helper-db-data-linear-string
    test-helper-db-data-random
    test-helper-db-data-random-string
    test-helper-db-database-delete
    test-helper-db-database-exit
    test-helper-db-database-init
    test-helper-db-database-path
    test-helper-db-database-path-root
    test-helper-db-database-reset
    test-helper-db-default-test-settings
    test-helper-db-estimate-btree-entry-count-relation
    test-helper-integer->bytevector
    test-helper-integer->string)
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

  (define next-integer (let (a 0) (nullary (set! a (+ a 1)) a)))
  (define last-monotonic-data-source 0)
  (define test-helper-db-database-path-root "/tmp/test/sph-dg")
  (define test-helper-db-database-path)
  (define test-helper-db-database-exit db-exit)

  (define (test-helper-db-database-path-next)
    (string-append test-helper-db-database-path-root "/" (number->string (next-integer))))

  (define (test-helper-db-count-btree-entries)
    (fold (l (e r) (+ (assoc-ref e (q ms-entries)) r)) 0 (db-txn-call-read db-statistics)))

  (define*
    (test-helper-db-estimate-btree-entry-count-relation count-l #:optional (count-r 1) (count-c 1))
    (+ (* 2 (* count-l count-r count-c)) (* count-l count-c)))

  (define (test-helper-db-create-relations count) "integer -> (list:left list:right)"
    (db-txn-call-write
      (l (txn)
        (let*
          ( (left (db-id-create txn 1)) (rights (db-id-create txn count))
            (status (db-relation-ensure txn left rights)))
          (list left rights)))))

  (define* (test-helper-db-data-random count #:optional (len 22))
    "integer [integer] -> (bytevector/string ...)"
    (map-integers count
      (l (n)
        (if (odd? (random 200))
          (string->utf8 (random-string (random len 1) char-set-vector:hex-digit))
          (random-string (random len 1) char-set-vector:hex-digit)))))

  (define* (test-helper-db-data-linear count)
    (let
      (r
        (map-integers count
          (l (n)
            ( (if (odd? n) test-helper-integer->string test-helper-integer->bytevector)
              (+ last-monotonic-data-source n)))))
      (set! last-monotonic-data-source (+ count last-monotonic-data-source)) r))

  (define* (test-helper-db-data-linear-string count)
    (let
      (r
        (map-integers count (l (n) (test-helper-integer->string (+ last-monotonic-data-source n)))))
      (set! last-monotonic-data-source (+ count last-monotonic-data-source)) r))

  (define* (test-helper-db-data-random-string count #:optional (len 22))
    (map-integers count (l (n) (random-string (random len 1) char-set-vector:hex-digit))))

  (define (test-helper-integer->bytevector a) "integer -> bytevector"
    (let (r (make-bytevector 8 0)) (bytevector-u64-native-set! r 0 a) r))

  (define (test-helper-integer->string a) "integer -> bytevector" (number->string a 32)))
