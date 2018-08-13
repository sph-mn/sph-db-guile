(library (test helper sph storage dg)
  (export
    test-helper-dg-add-default-test-settings
    test-helper-dg-count-btree-entries
    test-helper-dg-create-relations
    test-helper-dg-data-linear
    test-helper-dg-data-linear-string
    test-helper-dg-data-random
    test-helper-dg-data-random-string
    test-helper-dg-database-delete
    test-helper-dg-database-exit
    test-helper-dg-database-init
    test-helper-dg-database-path
    test-helper-dg-database-path-root
    test-helper-dg-database-reset
    test-helper-dg-default-test-settings
    test-helper-dg-estimate-btree-entry-count-relation
    test-helper-integer->bytevector
    test-helper-integer->string)
  (import
    (guile)
    (rnrs bytevectors)
    (only (rnrs base) set!)
    (sph)
    (sph alist)
    (sph char-set-vector)
    (sph one)
    (sph random-data)
    (sph storage dg)
    (sph storage dg implicit-txn)
    (only (sph list) map-integers))

  (define next-integer (let (a 0) (nullary (set! a (+ a 1)) a)))
  (define last-monotonic-data-source 0)
  (define test-helper-dg-database-path-root "/tmp/test/sph-dg")
  (define test-helper-dg-database-path)
  (define test-helper-dg-database-exit dg-exit)

  (define (test-helper-dg-default-test-settings settings)
    "add/append the default settings to the given test settings object.
     by default, the database is re-initialised before each procedure test.
     after each procedure test the intern and relation indexes are checked and the database is deleted"
    (let*
      ( (hook (alist-ref-q settings hook))
        (existing-procedure-before (alist-ref-q hook procedure-before))
        (existing-procedure-after (alist-ref-q hook procedure-after))
        (procedure-before (l a (test-helper-dg-database-reset)))
        (procedure-after
          (l a
            (if
              (or (alist-ref-q (dg-index-errors-intern*) errors?)
                (alist-ref-q (dg-index-errors-relation*) errors?))
              (raise (q index-corruption)))
            (test-helper-dg-database-delete))))
      (alist-set-multiple-q settings hook
        (alist-set-multiple-q (alist-ref-q settings hook) procedure-before
          (if existing-procedure-before
            (procedure-append-ignore-result existing-procedure-before procedure-before))
          procedure-after
          (if existing-procedure-after
            (procedure-append-ignore-result existing-procedure-after procedure-after) procedure-after)))))

  (define (test-helper-dg-database-path-next)
    (string-append test-helper-dg-database-path-root "/" (number->string (next-integer))))

  (define (test-helper-dg-database-init)
    (set! test-helper-dg-database-path (test-helper-dg-database-path-next))
    (dg-init test-helper-dg-database-path))

  (define (test-helper-dg-database-delete)
    (if (file-exists? test-helper-dg-database-path-root)
      (system* "rm" "-r" test-helper-dg-database-path-root)))

  (define (test-helper-dg-database-reset) (test-helper-dg-database-delete)
    (if (dg-initialised?) (dg-exit)) (test-helper-dg-database-init))

  (define (test-helper-dg-count-btree-entries)
    (fold (l (e r) (+ (assoc-ref e (q ms-entries)) r)) 0 (dg-txn-call-read dg-statistics)))

  (define*
    (test-helper-dg-estimate-btree-entry-count-relation count-l #:optional (count-r 1) (count-c 1))
    (+ (* 2 (* count-l count-r count-c)) (* count-l count-c)))

  (define (test-helper-dg-create-relations count) "integer -> (list:left list:right)"
    (dg-txn-call-write
      (l (txn)
        (let*
          ( (left (dg-id-create txn 1)) (rights (dg-id-create txn count))
            (status (dg-relation-ensure txn left rights)))
          (list left rights)))))

  (define* (test-helper-dg-data-random count #:optional (len 22))
    "integer [integer] -> (bytevector/string ...)"
    (map-integers count
      (l (n)
        (if (odd? (random 200))
          (string->utf8 (random-string (random len 1) char-set-vector:hex-digit))
          (random-string (random len 1) char-set-vector:hex-digit)))))

  (define* (test-helper-dg-data-linear count)
    (let
      (r
        (map-integers count
          (l (n)
            ( (if (odd? n) test-helper-integer->string test-helper-integer->bytevector)
              (+ last-monotonic-data-source n)))))
      (set! last-monotonic-data-source (+ count last-monotonic-data-source)) r))

  (define* (test-helper-dg-data-linear-string count)
    (let
      (r
        (map-integers count (l (n) (test-helper-integer->string (+ last-monotonic-data-source n)))))
      (set! last-monotonic-data-source (+ count last-monotonic-data-source)) r))

  (define* (test-helper-dg-data-random-string count #:optional (len 22))
    (map-integers count (l (n) (random-string (random len 1) char-set-vector:hex-digit))))

  (define (test-helper-integer->bytevector a) "integer -> bytevector"
    (let (r (make-bytevector 8 0)) (bytevector-u64-native-set! r 0 a) r))

  (define (test-helper-integer->string a) "integer -> bytevector" (number->string a 32)))
