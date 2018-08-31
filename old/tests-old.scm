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

  (define test-helper-common-element-count 30)
  (define test-helper-string-too-short "")

  (define test-helper-string-just-enough
    (random-string db-size-octets-data-max char-set-vector:hex-digit))

  (define test-helper-string-too-long
    (random-string (+ 1 db-size-octets-data-max) char-set-vector:hex-digit))

  (define-test (db-root) (assert-equal (dirname (db-root)) test-helper-db-database-path-root))

  (define-test (db-id-type?) "tests that the create element type is the expected and no other"
    (let*
      ( (id-creators
          (list (l (txn) (first (db-intern-ensure txn (list "a"))))
            (l (txn) (first (db-extern-create txn))) (l (txn) (first (db-id-create txn)))))
        (result-creators (list db-intern? db-extern? db-id?))
        (ids (db-txn-call-write (l (txn) (map (l (a) (a txn)) id-creators))))
        (results
          (map
            (l (proc id) (pair (proc id) (map (l (proc) (proc id)) (delete proc result-creators))))
            result-creators ids)))
      (assert-and (assert-true (every identity (map first results)))
        (assert-true (every not (apply append (map tail results)))))))

  (define-test (db-id-create)
    (let*
      ( (before-length (test-helper-db-count-btree-entries))
        (id-before
          (db-txn-call-write (l (txn) (db-id-create txn test-helper-common-element-count))))
        (identified-after (db-txn-call-read (l (txn) (db-identify txn id-before))))
        (after-length (test-helper-db-count-btree-entries)))
      (assert-and
        (assert-true "before-/after-length"
          (and (= 0 before-length) (= (length id-before) after-length)))
        (assert-true "existence"
          (and (length id-before) (length identified-after)
            (list-set-equal? id-before identified-after))))))

  (define-test (db-extern-create)
    (let*
      ( (count test-helper-common-element-count)
        (before-length (test-helper-db-count-btree-entries))
        (id-before
          (db-txn-call-write
            (l (txn) (append (db-extern-create txn count) (db-extern-create txn count "testdata")))))
        (identified-after (db-txn-call-read (l (txn) (db-identify txn id-before))))
        (after-length (test-helper-db-count-btree-entries)))
      (assert-and
        (assert-true "before-/after-length"
          (and (= 0 before-length) (= (+ (length id-before) count) (* 3 count) after-length)))
        (assert-true "existence"
          (and (length id-before) (length identified-after)
            (list-set-equal? id-before identified-after))))))

  (define-test (db-extern-data->id->data inp exp)
    (let*
      ( (data (first (test-helper-db-data-linear-string test-helper-common-element-count)))
        (before-count (test-helper-db-count-btree-entries))
        (id (db-txn-call-write (l (txn) (db-extern-create txn 1 data))))
        (after-count (test-helper-db-count-btree-entries))
        (after-data (db-txn-call-read (l (txn) (first (db-extern-id->data txn id (q string))))))
        (after-id (db-txn-call-read (l (txn) (db-extern-data->id txn data)))))
      (assert-and (assert-true (= after-count 2)) (assert-equal after-id id)
        (assert-equal after-data data))))

  (define-test (db-identify)
    (let*
      ( (ids (db-txn-call-write (l (txn) (db-id-create txn test-helper-common-element-count))))
        (r (db-txn-call-read (l (txn) (and (db-exists? txn ids) (db-identify txn ids))))))
      (if (and (= (length r) (length ids)) (equal? (reverse r) ids)) #t r)))

  (define-test (db-relation-ensure inp exp) "test creation"
    (let*
      ( (before-count (test-helper-db-count-btree-entries))
        (r (db-txn-call-write (l (txn) (apply db-relation-ensure txn inp))))
        (target-len
          (test-helper-db-estimate-btree-entry-count-relation (length (first inp))
            (length (first (tail inp)))))
        (after-count (test-helper-db-count-btree-entries)))
      (assert-and (assert-true (> after-count before-count))
        (assert-true (= target-len (- after-count before-count))))))

  (define-test (db-relation-ensure-2 inp exp) "test duplicate prevention"
    (apply
      (l (left right)
        (let*
          ( (before-count (test-helper-db-count-btree-entries))
            (status (db-txn-call-write (l (txn) (db-relation-ensure txn left right))))
            (after-count (test-helper-db-count-btree-entries)))
          (= before-count after-count)))
      (test-helper-db-create-relations test-helper-common-element-count)))

  (define-test (db-intern-ensure inp exp)
    (guard (a (#t (first a)))
      (let*
        ( (before-length (test-helper-db-count-btree-entries)) (r (db-intern-ensure* inp))
          (after-length (test-helper-db-count-btree-entries)))
        (assert-and
          ; count double because of the indexes
          (assert-equal (* 2 (length r)) (- after-length before-length)) (every integer? r)
          (= (length inp) (length r))
          (= (length (delete-duplicates inp)) (length (delete-duplicates r)))))))

  (define-test (db-intern-data->id inp exp)
    (let*
      ( (intern (test-helper-db-data-linear test-helper-common-element-count))
        (id (db-txn-call-write (l (txn) (db-intern-ensure txn intern))))
        (r (db-txn-call-read (l (txn) (db-intern-data->id txn intern)))))
      (if (and (= (length r) (length intern)) (equal? r id)) #t r)))

  (define-test (db-intern-id->data inp exp)
    (let*
      ( (intern (test-helper-db-data-linear-string test-helper-common-element-count))
        (id (db-txn-call-write (l (txn) (db-intern-ensure txn intern))))
        (r (db-txn-call-write (l (txn) (db-intern-id->data txn id (q string))))))
      (if (and (and (list? r) (list? id)) (= (length r) (length id)) (equal? r intern)) #t
        (list id r))))

  (define-test (db-intern-ensure-2) "test existing interns"
    (let*
      ( (intern (test-helper-db-data-linear test-helper-common-element-count))
        (r (db-txn-call-write (l (txn) (db-intern-ensure txn intern))))
        (r-2 (db-txn-call-write (l (txn) (db-intern-ensure txn intern)))))
      (if (null? (difference r r-2)) #t
        (list (length (difference r r-2)) (length (delete-duplicates (append r r-2)))))))

  (define-test (db-intern-ensure-3 inp exp) "test read type conversions"
    (let*
      ( (integers (list 2 -3 400)) (strings (list "abc" "de" "fghi"))
        (bytevectors (map string->utf8 strings))
        (r
          (list
            (equal? strings
              (db-txn-call-write
                (l (txn) (db-intern-id->data txn (db-intern-ensure txn strings) (q string)))))
            (equal? strings
              (map (l (e) (string-trim-right (utf8->string e) #\nul))
                (db-txn-call-write
                  (l (txn)
                    (db-intern-id->data txn (db-intern-ensure txn bytevectors) (q bytevector))))))
            (equal? integers
              (db-txn-call-write
                (l (txn) (db-intern-id->data txn (db-intern-ensure txn integers) (q integer)))))
            (let (serialisable-data (list #t (vector 1 (pair 2 3))))
              (equal? serialisable-data
                (db-txn-call-write
                  (l (txn)
                    (db-intern-id->data txn (db-intern-ensure txn serialisable-data) (q scheme)))))))))
      (if (every identity r) exp r)))

  (define (get-tree-entry-count . tree-names)
    (let (stat-info (db-txn-call-read (l (txn) (db-statistics txn))))
      (fold
        (l (e prev)
          (let (r (assoc-ref stat-info e)) (if r (pair (assoc-ref r (q ms-entries)) prev) prev)))
        (list) tree-names)))

  (define-test (db-delete-id)
    (let*
      ( (before-length (test-helper-db-count-btree-entries))
        (id (db-txn-call-write (l (txn) (db-id-create txn test-helper-common-element-count))))
        (identified (db-txn-call-read (l (txn) (db-identify txn id))))
        (identified-2 (db-txn-call-write (l (txn) (db-delete txn id) (db-identify txn id))))
        (after-length (test-helper-db-count-btree-entries)))
      (if
        (and (= (length identified) (length id)) (null? identified-2)
          (= before-length after-length))
        #t (pairs before-length after-length (map length (list id identified identified-2))))))

  (define-test (db-delete-extern)
    (let*
      ( (before-length (test-helper-db-count-btree-entries))
        (id
          (db-txn-call-write
            (l (txn)
              (append (db-extern-create txn test-helper-common-element-count)
                (db-extern-create txn test-helper-common-element-count "testdata")))))
        (identified (db-txn-call-read (l (txn) (db-identify txn id))))
        (identified-2
          (db-txn-call-write (l (txn) (db-delete txn id) (filter identity (db-identify txn id)))))
        (after-length (test-helper-db-count-btree-entries)))
      (if
        (and (= (length identified) (length id)) (null? identified-2)
          (= before-length after-length))
        #t (map length (list id identified identified-2)))))

  (define-test (db-delete-intern)
    (let*
      ( (intern (map-integers test-helper-common-element-count (l (n) (number->string n 32))))
        (before-length (test-helper-db-count-btree-entries))
        (id
          (db-txn-call-write
            (l (txn) (db-intern-ensure txn intern) (db-intern-data->id txn intern))))
        (id-2 (db-txn-call-write (l (txn) (db-delete txn id) (db-intern-data->id txn intern #f))))
        (after-length (test-helper-db-count-btree-entries)))
      (assert-and (assert-true "all created" (= (length intern) (length id)))
        ;all intern deleted
        (assert-true "all nonexistant" (null? id-2))
        (assert-true "entry count" (= before-length after-length)))))

  (define (call-with-left-right proc)
    (let
      (a
        (any->list
          (db-txn-call-write
            (l (txn)
              (let*
                ((a (db-id-create txn 1)) (b (db-id-create txn test-helper-common-element-count)))
                (list a b))))))
      (apply proc a)))

  (define (db-relation-delete-argument-permutations left right label)
    (remove (l (a) (every not a)) (produce list (list left #f) (list right #f) (list label #f))))

  (define test-db-relation-delete
    (let
      (prepare
        (l (c)
          (call-with-left-right
            (l (left right)
              (c left right (db-relation-delete-argument-permutations left right (list 0)))))))
      (l a
        (let
          (results
            (prepare
              (l (left right permutations)
                (let
                  (results
                    (map
                      (l (arguments)
                        (let*
                          ( (before-create-count (test-helper-db-count-btree-entries))
                            (status
                              (db-txn-call-write (l (txn) (db-relation-ensure txn left right #f))))
                            (after-create-count (test-helper-db-count-btree-entries))
                            (status
                              (db-txn-call-write (l (txn) (apply db-relation-delete txn arguments))))
                            (after-delete-count (test-helper-db-count-btree-entries))
                            (reader-suffix
                              (db-relation-read-argument-permutation->reader-suffix arguments)))
                          (bindings->alist reader-suffix before-create-count
                            after-create-count after-delete-count)))
                      permutations))
                  (map
                    (l (a)
                      (alist-bind a
                        (reader-suffix before-create-count after-create-count after-delete-count)
                        (list reader-suffix
                          (or (> after-create-count before-create-count) (q creation-failed))
                          (or (< after-delete-count after-create-count) (q deletion-failed)))))
                    results)))))
          (every (l (a) (every identity (tail a))) results)))))

  (define-test (db-delete-references)
    (apply
      (l (left right)
        (let*
          ( (r (db-txn-call-write (l (txn) (db-delete txn (append left right)))))
            (after-length (test-helper-db-count-btree-entries)))
          (if (and (= 0 after-length)) #t
            (list (q after-length) after-length
              (q distribution) (test-helper-db-count-btree-entries)))))
      (test-helper-db-create-relations test-helper-common-element-count)))

  (define-test (db-index-recreate-pair)
    (apply
      (l (left right)
        (assert-and
          (assert-true "preparation"
            (list-set-equal? pairs
              (map vector-first
                (db-txn-call-read
                  (l (txn) (db-relation-read (db-relation-select txn pairs) (length pairs)))))))
          (assert-and
            (begin (db-index-recreate-pair)
              (assert-true
                (list-set-equal? pairs
                  (map vector-first
                    (db-txn-call-read (l (txn) (db-relation-read (db-relation-select txn pairs))))))))
            (assert-true
              (not (alist-ref-q (db-txn-call-read (l (txn) (db-index-errors-pair txn))) error?))))))
      (test-helper-db-create-relations test-helper-common-element-count)))

  (define-test (db-index-recreate-intern)
    (let*
      ( (test-count test-helper-common-element-count)
        (test-values-1 (test-helper-db-data-linear-string test-count)))
      (let*
        ( (test-values-2 (test-helper-db-data-linear-string test-count))
          (test-intern-1 (db-txn-call-write (l (txn) (db-intern-ensure txn test-values-1))))
          (test-intern-2 (db-txn-call-write (l (txn) (db-intern-ensure txn test-values-2)))))
        (assert-and
          (assert-true "preparation"
            (db-txn-call-read
              (l (txn)
                (and
                  (list-set-equal? test-values-1 (db-intern-id->data txn test-intern-1 (q string)))
                  (list-set-equal? test-values-2 (db-intern-id->data txn test-intern-2 (q string)))))))
          (begin (db-index-recreate-intern) #t)
          (db-txn-call-read
            (l (txn)
              (assert-and
                (assert-true
                  (list-set-equal? test-values-1 (db-intern-id->data txn test-intern-1 (q string))))
                (assert-true
                  (list-set-equal? test-values-2 (db-intern-id->data txn test-intern-2 (q string))))
                (assert-true "index check" (not (alist-ref-q (db-index-errors-intern txn) errors?))))))))))

  (define-test (db-node-read)
    (let (test-values (test-helper-db-data-linear test-helper-common-element-count))
      (db-txn-call-write
        (l (txn) (db-intern-ensure txn test-values) (db-id-create txn (length test-values))))
      (db-txn-call-read
        (l (txn)
          (let*
            ( (selection (db-node-select txn (db-types intern)))
              (records (append (db-node-read selection) (db-node-read selection 2))))
            (assert-true
              (every
                (l (a) (and (= 2 (vector-length a)) (db-intern? (vector-ref a 0)) (vector-ref a 1)))
                records)))))))

  (define-test (db-intern-update)
    (let*
      ( (intern-a (test-helper-db-data-linear-string test-helper-common-element-count))
        (intern-b (test-helper-db-data-linear-string (length intern-a)))
        (id-a (db-txn-call-write (l (txn) (db-intern-ensure txn intern-a))))
        (result-update
          (every (l (id new) (db-txn-call-write (l (txn) (db-intern-update txn id new)))) id-a
            intern-b))
        (id-old (db-txn-call-read (l (txn) (db-intern-data->id txn intern-a #f))))
        (id-new (db-txn-call-read (l (txn) (db-intern-data->id txn intern-b))))
        (duplicate-prevented?
          (equal? (q duplicate)
            (guard (a (#t (first a)))
              (db-txn-call-write (l (txn) (db-intern-update txn (first id-a) (first intern-b))))))))
      (assert-and (assert-true result-update) (assert-true (list? id-new))
        (assert-true (every integer? id-new)) (assert-true (null? id-old))
        (assert-true (= (length id-new) (length id-a))) (assert-true duplicate-prevented?))))

  (define-test (multiple-selections)
    (apply
      (l (left right)
        (db-txn-call-read
          (l (txn)
            (let (selections (map-integers 200 (l (n) (db-relation-select txn #f left right))))
              (assert-true (every list? (map (l (s) (db-relation-read s)) selections)))))))
      (test-helper-db-create-relations test-helper-common-element-count)))

  (define-test (read-gc)
    ;"this is not as relevant anymore as it was in the past. in past versions,
    ;selections including cursors where freed on garbage collection"
    (db-txn-call-write
      (l (txn)
        (db-intern-ensure txn (test-helper-db-data-linear-string test-helper-common-element-count))
        (db-relation-ensure txn (db-id-create txn) (db-id-create txn))))
    (db-txn-call-read
      (l (txn)
        (let (selections (list (db-relation-select txn (list 2)) (db-node-select txn)))
          (assert-true (every db-selection? selections) (begin (gc) #t))))))

  (define (map-map proc . lists) (apply map (l e (apply map proc e)) lists))

  (define (db-relation-read-argument-permutations left right label)
    (remove (l (a) (every null? a)) (produce list (list left #f) (list right #f) (list label #f))))

  (define (db-relation-read-argument-permutation->reader-suffix a)
    (string->number (apply string-append (map (l (a) (if (null? a) "0" "1")) a))))

  (define-test (db-relation-read)
    (apply
      (l (left right)
        (let*
          ( (argument-permutations
              (list (first (db-relation-read-argument-permutations left right (list 0)))))
            (results
              (db-txn-call-read
                (l (txn)
                  (map (l (a) (db-relation-read (apply db-relation-select txn a)))
                    argument-permutations))))
            (result-lengths (map (l (a) (if (list? a) (length a) 0)) results)))
          (assert-and (assert-true (every (l (a) (every vector? a)) results))
            (assert-true (every (l (a) (every (l (a) (= 4 (vector-length a))) a)) results))
            (assert-true
              (every (l (a) (every (l (a) (every integer? (vector->list a))) a)) results)))))
      (test-helper-db-create-relations test-helper-common-element-count)))

  (define-procedure-tests tests (db-root)
    (db-relation-delete)
    (db-intern-ensure (unquote test-helper-string-too-long) data-length
      (unquote test-helper-string-just-enough) #t)
    (db-intern-update) (db-id-type?)
    (db-intern-id->data) (db-intern-ensure-2)
    (db-relation-read) (multiple-selections)
    (db-relation-ensure ((1 2 3) (4 5 6 7)) #t) (db-relation-ensure-2)
    (db-delete-id) (db-identify)
    (db-id-create) (db-intern-ensure-3)
    (db-extern-data->id->data) (db-extern-create)
    (db-intern-data->id) (db-node-read)
    (db-index-recreate-intern) (db-index-recreate-relation)
    (db-delete-intern) (db-delete-extern) (read-gc) (db-delete-references))

  (l (settings)
    (let*
      ( (settings
          (alist-set-multiple-q (test-helper-db-default-test-settings settings) exception->key #t))
        (result (apply append (map-integers 1 (l (n) (test-execute-procedures settings tests))))))
      (test-helper-db-database-exit) result)))
