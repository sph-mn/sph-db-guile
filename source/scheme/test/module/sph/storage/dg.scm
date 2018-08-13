(define-test-module (test module sph storage dg)
  (import
    (sph storage dg)
    (test helper sph storage dg)
    (sph common)
    (sph char-set-vector)
    (ice-9 threads)
    (rnrs bytevectors)
    (sph random-data)
    (srfi srfi-41)
    (sph storage dg implicit-txn)
    (sph stream)
    (sph vector selection))

  (define test-helper-common-element-count 30)
  (define test-helper-string-too-short "")

  (define test-helper-string-just-enough
    (random-string dg-size-octets-data-max char-set-vector:hex-digit))

  (define test-helper-string-too-long
    (random-string (+ 1 dg-size-octets-data-max) char-set-vector:hex-digit))

  (define-test (dg-root) (assert-equal (dirname (dg-root)) test-helper-dg-database-path-root))

  (define-test (dg-id-type?) "tests that the create element type is the expected and no other"
    (let*
      ( (id-creators
          (list (l (txn) (first (dg-intern-ensure txn (list "a"))))
            (l (txn) (first (dg-extern-create txn))) (l (txn) (first (dg-id-create txn)))))
        (result-creators (list dg-intern? dg-extern? dg-id?))
        (ids (dg-txn-call-write (l (txn) (map (l (a) (a txn)) id-creators))))
        (results
          (map
            (l (proc id) (pair (proc id) (map (l (proc) (proc id)) (delete proc result-creators))))
            result-creators ids)))
      (assert-and (assert-true (every identity (map first results)))
        (assert-true (every not (apply append (map tail results)))))))

  (define-test (dg-id-create)
    (let*
      ( (before-length (test-helper-dg-count-btree-entries))
        (id-before
          (dg-txn-call-write (l (txn) (dg-id-create txn test-helper-common-element-count))))
        (identified-after (dg-txn-call-read (l (txn) (dg-identify txn id-before))))
        (after-length (test-helper-dg-count-btree-entries)))
      (assert-and
        (assert-true "before-/after-length"
          (and (= 0 before-length) (= (length id-before) after-length)))
        (assert-true "existence"
          (and (length id-before) (length identified-after)
            (list-set-equal? id-before identified-after))))))

  (define-test (dg-extern-create)
    (let*
      ( (count test-helper-common-element-count)
        (before-length (test-helper-dg-count-btree-entries))
        (id-before
          (dg-txn-call-write
            (l (txn) (append (dg-extern-create txn count) (dg-extern-create txn count "testdata")))))
        (identified-after (dg-txn-call-read (l (txn) (dg-identify txn id-before))))
        (after-length (test-helper-dg-count-btree-entries)))
      (assert-and
        (assert-true "before-/after-length"
          (and (= 0 before-length) (= (+ (length id-before) count) (* 3 count) after-length)))
        (assert-true "existence"
          (and (length id-before) (length identified-after)
            (list-set-equal? id-before identified-after))))))

  (define-test (dg-extern-data->id->data inp exp)
    (let*
      ( (data (first (test-helper-dg-data-linear-string test-helper-common-element-count)))
        (before-count (test-helper-dg-count-btree-entries))
        (id (dg-txn-call-write (l (txn) (dg-extern-create txn 1 data))))
        (after-count (test-helper-dg-count-btree-entries))
        (after-data (dg-txn-call-read (l (txn) (first (dg-extern-id->data txn id (q string))))))
        (after-id (dg-txn-call-read (l (txn) (dg-extern-data->id txn data)))))
      (assert-and (assert-true (= after-count 2)) (assert-equal after-id id)
        (assert-equal after-data data))))

  (define-test (dg-identify)
    (let*
      ( (ids (dg-txn-call-write (l (txn) (dg-id-create txn test-helper-common-element-count))))
        (r (dg-txn-call-read (l (txn) (and (dg-exists? txn ids) (dg-identify txn ids))))))
      (if (and (= (length r) (length ids)) (equal? (reverse r) ids)) #t r)))

  (define-test (dg-relation-ensure inp exp) "test creation"
    (let*
      ( (before-count (test-helper-dg-count-btree-entries))
        (r (dg-txn-call-write (l (txn) (apply dg-relation-ensure txn inp))))
        (target-len
          (test-helper-dg-estimate-btree-entry-count-relation (length (first inp))
            (length (first (tail inp)))))
        (after-count (test-helper-dg-count-btree-entries)))
      (assert-and (assert-true (> after-count before-count))
        (assert-true (= target-len (- after-count before-count))))))

  (define-test (dg-relation-ensure-2 inp exp) "test duplicate prevention"
    (apply
      (l (left right)
        (let*
          ( (before-count (test-helper-dg-count-btree-entries))
            (status (dg-txn-call-write (l (txn) (dg-relation-ensure txn left right))))
            (after-count (test-helper-dg-count-btree-entries)))
          (= before-count after-count)))
      (test-helper-dg-create-relations test-helper-common-element-count)))

  (define-test (dg-intern-ensure inp exp)
    (guard (a (#t (first a)))
      (let*
        ( (before-length (test-helper-dg-count-btree-entries)) (r (dg-intern-ensure* inp))
          (after-length (test-helper-dg-count-btree-entries)))
        (assert-and
          ; count double because of the indexes
          (assert-equal (* 2 (length r)) (- after-length before-length)) (every integer? r)
          (= (length inp) (length r))
          (= (length (delete-duplicates inp)) (length (delete-duplicates r)))))))

  (define-test (dg-intern-data->id inp exp)
    (let*
      ( (intern (test-helper-dg-data-linear test-helper-common-element-count))
        (id (dg-txn-call-write (l (txn) (dg-intern-ensure txn intern))))
        (r (dg-txn-call-read (l (txn) (dg-intern-data->id txn intern)))))
      (if (and (= (length r) (length intern)) (equal? r id)) #t r)))

  (define-test (dg-intern-id->data inp exp)
    (let*
      ( (intern (test-helper-dg-data-linear-string test-helper-common-element-count))
        (id (dg-txn-call-write (l (txn) (dg-intern-ensure txn intern))))
        (r (dg-txn-call-write (l (txn) (dg-intern-id->data txn id (q string))))))
      (if (and (and (list? r) (list? id)) (= (length r) (length id)) (equal? r intern)) #t
        (list id r))))

  (define-test (dg-intern-ensure-2) "test existing interns"
    (let*
      ( (intern (test-helper-dg-data-linear test-helper-common-element-count))
        (r (dg-txn-call-write (l (txn) (dg-intern-ensure txn intern))))
        (r-2 (dg-txn-call-write (l (txn) (dg-intern-ensure txn intern)))))
      (if (null? (difference r r-2)) #t
        (list (length (difference r r-2)) (length (delete-duplicates (append r r-2)))))))

  (define-test (dg-intern-ensure-3 inp exp) "test read type conversions"
    (let*
      ( (integers (list 2 -3 400)) (strings (list "abc" "de" "fghi"))
        (bytevectors (map string->utf8 strings))
        (r
          (list
            (equal? strings
              (dg-txn-call-write
                (l (txn) (dg-intern-id->data txn (dg-intern-ensure txn strings) (q string)))))
            (equal? strings
              (map (l (e) (string-trim-right (utf8->string e) #\nul))
                (dg-txn-call-write
                  (l (txn)
                    (dg-intern-id->data txn (dg-intern-ensure txn bytevectors) (q bytevector))))))
            (equal? integers
              (dg-txn-call-write
                (l (txn) (dg-intern-id->data txn (dg-intern-ensure txn integers) (q integer)))))
            (let (serialisable-data (list #t (vector 1 (pair 2 3))))
              (equal? serialisable-data
                (dg-txn-call-write
                  (l (txn)
                    (dg-intern-id->data txn (dg-intern-ensure txn serialisable-data) (q scheme)))))))))
      (if (every identity r) exp r)))

  (define (get-tree-entry-count . tree-names)
    (let (stat-info (dg-txn-call-read (l (txn) (dg-statistics txn))))
      (fold
        (l (e prev)
          (let (r (assoc-ref stat-info e)) (if r (pair (assoc-ref r (q ms-entries)) prev) prev)))
        (list) tree-names)))

  (define-test (dg-delete-id)
    (let*
      ( (before-length (test-helper-dg-count-btree-entries))
        (id (dg-txn-call-write (l (txn) (dg-id-create txn test-helper-common-element-count))))
        (identified (dg-txn-call-read (l (txn) (dg-identify txn id))))
        (identified-2 (dg-txn-call-write (l (txn) (dg-delete txn id) (dg-identify txn id))))
        (after-length (test-helper-dg-count-btree-entries)))
      (if
        (and (= (length identified) (length id)) (null? identified-2)
          (= before-length after-length))
        #t (pairs before-length after-length (map length (list id identified identified-2))))))

  (define-test (dg-delete-extern)
    (let*
      ( (before-length (test-helper-dg-count-btree-entries))
        (id
          (dg-txn-call-write
            (l (txn)
              (append (dg-extern-create txn test-helper-common-element-count)
                (dg-extern-create txn test-helper-common-element-count "testdata")))))
        (identified (dg-txn-call-read (l (txn) (dg-identify txn id))))
        (identified-2
          (dg-txn-call-write (l (txn) (dg-delete txn id) (filter identity (dg-identify txn id)))))
        (after-length (test-helper-dg-count-btree-entries)))
      (if
        (and (= (length identified) (length id)) (null? identified-2)
          (= before-length after-length))
        #t (map length (list id identified identified-2)))))

  (define-test (dg-delete-intern)
    (let*
      ( (intern (map-integers test-helper-common-element-count (l (n) (number->string n 32))))
        (before-length (test-helper-dg-count-btree-entries))
        (id
          (dg-txn-call-write
            (l (txn) (dg-intern-ensure txn intern) (dg-intern-data->id txn intern))))
        (id-2 (dg-txn-call-write (l (txn) (dg-delete txn id) (dg-intern-data->id txn intern #f))))
        (after-length (test-helper-dg-count-btree-entries)))
      (assert-and (assert-true "all created" (= (length intern) (length id)))
        ;all intern deleted
        (assert-true "all nonexistant" (null? id-2))
        (assert-true "entry count" (= before-length after-length)))))

  (define (call-with-left-right proc)
    (let
      (a
        (any->list
          (dg-txn-call-write
            (l (txn)
              (let*
                ((a (dg-id-create txn 1)) (b (dg-id-create txn test-helper-common-element-count)))
                (list a b))))))
      (apply proc a)))

  (define (dg-relation-delete-argument-permutations left right label)
    (remove (l (a) (every not a)) (produce list (list left #f) (list right #f) (list label #f))))

  (define test-dg-relation-delete
    (let
      (prepare
        (l (c)
          (call-with-left-right
            (l (left right)
              (c left right (dg-relation-delete-argument-permutations left right (list 0)))))))
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
                          ( (before-create-count (test-helper-dg-count-btree-entries))
                            (status
                              (dg-txn-call-write (l (txn) (dg-relation-ensure txn left right #f))))
                            (after-create-count (test-helper-dg-count-btree-entries))
                            (status
                              (dg-txn-call-write (l (txn) (apply dg-relation-delete txn arguments))))
                            (after-delete-count (test-helper-dg-count-btree-entries))
                            (reader-suffix
                              (dg-relation-read-argument-permutation->reader-suffix arguments)))
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

  (define-test (dg-delete-references)
    (apply
      (l (left right)
        (let*
          ( (r (dg-txn-call-write (l (txn) (dg-delete txn (append left right)))))
            (after-length (test-helper-dg-count-btree-entries)))
          (if (and (= 0 after-length)) #t
            (list (q after-length) after-length
              (q distribution) (test-helper-dg-count-btree-entries)))))
      (test-helper-dg-create-relations test-helper-common-element-count)))

  (define-test (dg-index-recreate-pair)
    (apply
      (l (left right)
        (assert-and
          (assert-true "preparation"
            (list-set-equal? pairs
              (map vector-first
                (dg-txn-call-read
                  (l (txn) (dg-relation-read (dg-relation-select txn pairs) (length pairs)))))))
          (assert-and
            (begin (dg-index-recreate-pair)
              (assert-true
                (list-set-equal? pairs
                  (map vector-first
                    (dg-txn-call-read (l (txn) (dg-relation-read (dg-relation-select txn pairs))))))))
            (assert-true
              (not (alist-ref-q (dg-txn-call-read (l (txn) (dg-index-errors-pair txn))) error?))))))
      (test-helper-dg-create-relations test-helper-common-element-count)))

  (define-test (dg-index-recreate-intern)
    (let*
      ( (test-count test-helper-common-element-count)
        (test-values-1 (test-helper-dg-data-linear-string test-count)))
      (let*
        ( (test-values-2 (test-helper-dg-data-linear-string test-count))
          (test-intern-1 (dg-txn-call-write (l (txn) (dg-intern-ensure txn test-values-1))))
          (test-intern-2 (dg-txn-call-write (l (txn) (dg-intern-ensure txn test-values-2)))))
        (assert-and
          (assert-true "preparation"
            (dg-txn-call-read
              (l (txn)
                (and
                  (list-set-equal? test-values-1 (dg-intern-id->data txn test-intern-1 (q string)))
                  (list-set-equal? test-values-2 (dg-intern-id->data txn test-intern-2 (q string)))))))
          (begin (dg-index-recreate-intern) #t)
          (dg-txn-call-read
            (l (txn)
              (assert-and
                (assert-true
                  (list-set-equal? test-values-1 (dg-intern-id->data txn test-intern-1 (q string))))
                (assert-true
                  (list-set-equal? test-values-2 (dg-intern-id->data txn test-intern-2 (q string))))
                (assert-true "index check" (not (alist-ref-q (dg-index-errors-intern txn) errors?))))))))))

  (define-test (dg-node-read)
    (let (test-values (test-helper-dg-data-linear test-helper-common-element-count))
      (dg-txn-call-write
        (l (txn) (dg-intern-ensure txn test-values) (dg-id-create txn (length test-values))))
      (dg-txn-call-read
        (l (txn)
          (let*
            ( (selection (dg-node-select txn (dg-types intern)))
              (records (append (dg-node-read selection) (dg-node-read selection 2))))
            (assert-true
              (every
                (l (a) (and (= 2 (vector-length a)) (dg-intern? (vector-ref a 0)) (vector-ref a 1)))
                records)))))))

  (define-test (dg-intern-update)
    (let*
      ( (intern-a (test-helper-dg-data-linear-string test-helper-common-element-count))
        (intern-b (test-helper-dg-data-linear-string (length intern-a)))
        (id-a (dg-txn-call-write (l (txn) (dg-intern-ensure txn intern-a))))
        (result-update
          (every (l (id new) (dg-txn-call-write (l (txn) (dg-intern-update txn id new)))) id-a
            intern-b))
        (id-old (dg-txn-call-read (l (txn) (dg-intern-data->id txn intern-a #f))))
        (id-new (dg-txn-call-read (l (txn) (dg-intern-data->id txn intern-b))))
        (duplicate-prevented?
          (equal? (q duplicate)
            (guard (a (#t (first a)))
              (dg-txn-call-write (l (txn) (dg-intern-update txn (first id-a) (first intern-b))))))))
      (assert-and (assert-true result-update) (assert-true (list? id-new))
        (assert-true (every integer? id-new)) (assert-true (null? id-old))
        (assert-true (= (length id-new) (length id-a))) (assert-true duplicate-prevented?))))

  (define-test (multiple-selections)
    (apply
      (l (left right)
        (dg-txn-call-read
          (l (txn)
            (let (selections (map-integers 200 (l (n) (dg-relation-select txn #f left right))))
              (assert-true (every list? (map (l (s) (dg-relation-read s)) selections)))))))
      (test-helper-dg-create-relations test-helper-common-element-count)))

  (define-test (read-gc)
    ;"this is not as relevant anymore as it was in the past. in past versions,
    ;selections including cursors where freed on garbage collection"
    (dg-txn-call-write
      (l (txn)
        (dg-intern-ensure txn (test-helper-dg-data-linear-string test-helper-common-element-count))
        (dg-relation-ensure txn (dg-id-create txn) (dg-id-create txn))))
    (dg-txn-call-read
      (l (txn)
        (let (selections (list (dg-relation-select txn (list 2)) (dg-node-select txn)))
          (assert-true (every dg-selection? selections) (begin (gc) #t))))))

  (define (map-map proc . lists) (apply map (l e (apply map proc e)) lists))

  (define (dg-relation-read-argument-permutations left right label)
    (remove (l (a) (every null? a)) (produce list (list left #f) (list right #f) (list label #f))))

  (define (dg-relation-read-argument-permutation->reader-suffix a)
    (string->number (apply string-append (map (l (a) (if (null? a) "0" "1")) a))))

  (define-test (dg-relation-read)
    (apply
      (l (left right)
        (let*
          ( (argument-permutations
              (list (first (dg-relation-read-argument-permutations left right (list 0)))))
            (results
              (dg-txn-call-read
                (l (txn)
                  (map (l (a) (dg-relation-read (apply dg-relation-select txn a)))
                    argument-permutations))))
            (result-lengths (map (l (a) (if (list? a) (length a) 0)) results)))
          (assert-and (assert-true (every (l (a) (every vector? a)) results))
            (assert-true (every (l (a) (every (l (a) (= 4 (vector-length a))) a)) results))
            (assert-true
              (every (l (a) (every (l (a) (every integer? (vector->list a))) a)) results)))))
      (test-helper-dg-create-relations test-helper-common-element-count)))

  (define-procedure-tests tests (dg-root)
    (dg-relation-delete)
    (dg-intern-ensure (unquote test-helper-string-too-long) data-length
      (unquote test-helper-string-just-enough) #t)
    (dg-intern-update) (dg-id-type?)
    (dg-intern-id->data) (dg-intern-ensure-2)
    (dg-relation-read) (multiple-selections)
    (dg-relation-ensure ((1 2 3) (4 5 6 7)) #t) (dg-relation-ensure-2)
    (dg-delete-id) (dg-identify)
    (dg-id-create) (dg-intern-ensure-3)
    (dg-extern-data->id->data) (dg-extern-create)
    (dg-intern-data->id) (dg-node-read)
    (dg-index-recreate-intern) (dg-index-recreate-relation)
    (dg-delete-intern) (dg-delete-extern) (read-gc) (dg-delete-references))

  (l (settings)
    (let*
      ( (settings
          (alist-set-multiple-q (test-helper-dg-default-test-settings settings) exception->key #t))
        (result (apply append (map-integers 1 (l (n) (test-execute-procedures settings tests))))))
      (test-helper-dg-database-exit) result)))
