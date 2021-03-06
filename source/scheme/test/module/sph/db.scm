(define-test-module (test module sph db)
  (import
    (sph db)
    (rnrs bytevectors)
    (sph list)
    (sph alist)
    (only (srfi srfi-1) every lset-difference any)
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
          (null? (db-type-indices type)) (not (not (db-type-get env "type-1")))
          (begin (db-type-delete env type) (not (db-type-get env "type-1")))))))

  (define-test (db-index env)
    (test-helper-type-create-1 env
      (l (type-name type-fields type)
        (let* ((index-fields (list 1 "field-3")) (index (db-index-create env type index-fields)))
          (assert-and (not (not index))
            (equal? (db-index-fields index) (db-index-fields (db-index-get env type index-fields)))
            (begin (db-index-rebuild env index) #t) (begin (db-index-delete env index) #t)
            (not (db-index-get env type index-fields)))))))

  (define-test (db-relation-ensure env)
    (db-txn-call-write env
      (l (txn)
        (let ((left (list 1 2 3)) (right (list 4 5)) (label (list 7)))
          (db-relation-ensure txn left right label (l (a) (list a (+ 1 a))) 2)))))

  (define common-element-count 100)

  (define-test (db-record-create env)
    (test-helper-type-create-1 env
      (l (type-name type-1-fields type-1)
        (test-helper-records-create-1 common-element-count env
          type-1
          (l (ids-and-values)
            (let ((ids (map first ids-and-values)) (values (map tail ids-and-values)))
              (db-txn-call-write env
                (l (txn)
                  (assert-and (every integer? ids)
                    (= common-element-count (length (db-record-get txn ids)))
                    (assert-true "record-get and value compare"
                      (let*
                        ( (records (db-record-get txn ids))
                          (values-read
                            (map
                              (l (a)
                                (compact
                                  (map-with-index (l (index a) (and a (pair index a)))
                                    (vector->list (db-record->vector type-1 a)))))
                              records)))
                        (every
                          (l (values)
                            (any
                              (l (values-read)
                                (every
                                  (l (value)
                                    (any (l (value-read) (equal? value-read value)) values-read))
                                  values))
                              values-read))
                          values))))))))))))

  (define-test (db-record-select env)
    (test-helper-type-create-1 env
      (l (type-name type-1-fields type-1)
        (test-helper-records-create-1 common-element-count env
          type-1
          (l (ids-and-values)
            (db-txn-call-write env
              (l (txn)
                (let (selection (db-record-select txn type-1))
                  (every vector?
                    (map (l (a) (db-record->vector type-1 a))
                      (append (db-record-read selection 1)
                        (db-record-read selection (* 2 common-element-count))))))
                (let
                  (selection
                    (db-record-select txn type-1 (l (type record . state) (pair #t state))))
                  (every vector?
                    (map (l (a) (db-record->vector type-1 a))
                      (append (db-record-read selection 1)
                        (db-record-read selection (* 2 common-element-count)))))))))))))

  (define-test (db-relation-select env)
    (test-helper-relations-create-1 env
      (l (left right label)
        (db-txn-call-write env
          (l (txn)
            (let*
              ( (selection (db-relation-select txn left right label))
                (results (db-relation-read selection 100)))
              (and (not (null? results)) (every vector? results))))))))

  (define-test (db-record-virtual env)
    "float currently not supported as guile only supports 64 bit float which dont fit in ids"
    (let*
      ( (type-uint (db-type-create env "vtype-uint" (q (uint16f)) db-type-flag-virtual))
        (type-int (db-type-create env "vtype-int" (q (int8f)) db-type-flag-virtual))
        (type-string (db-type-create env "vtype-string16f" (q (string8f)) db-type-flag-virtual))
        (type-binary (db-type-create env "vtype-binary16f" (q (binary8f)) db-type-flag-virtual)))
      (assert-and (every db-type-virtual? (list type-uint type-int type-string type-binary))
        (every
          (l (a)
            (let ((type (first a)) (value (tail a)))
              (equal? value (db-record-virtual-data type (db-record-virtual type value)))))
          (list (pair type-uint 123) (pair type-int -123)
            (pair type-string "1") (pair type-binary (string->utf8 "1")))))))

  (define-test (db-other env)
    (let* ((id-element 123) (id-type 6500) (id (db-id-add-type id-element id-type)))
      (and (= id-element (db-id-element id)) (= id-type (db-id-type id)))))

  (define-test (db-record-update env)
    (apply
      (l (type id old new)
        (db-txn-call-write env
          (l (txn) (db-record-update txn type id new)
            (let (read (db-record->values type (first (db-record-get txn (list id)))))
              (null?
                (lset-difference
                  (l (a b) (if (= (first a) (first b)) (equal? (tail a) (tail b)) #t)) old new))))))
      (test-helper-type-create-1 env
        (l (type-name type-1-fields type-1)
          (let
            ( (values-old (q ((0 . -123) (1 . 123) (3 . 1.23) (4 . "123"))))
              (values-new (q ((0 . 1) (1 . 2) (3 . 4.5) (4 . "3")))))
            (db-txn-call-write env
              (l (txn) (list type-1 (db-record-create txn type-1 values-old) values-old values-new))))))))

  (define-test (db-index-select env)
    (let ((index-fields-1 (list 0 1)) (index-fields-2 (list 0 4)))
      (test-helper-type-create-1 env
        (l (type-name type-fields type) (db-index-create env type index-fields-1)
          (test-helper-records-create-2 2 env
            type
            (l (ids-and-values)
              (let (index (db-index-create env type index-fields-2))
                (db-txn-call-write env
                  (l (txn)
                    (let (id-and-values (first ids-and-values))
                      (null?
                        (lset-difference equal?
                          (list (first id-and-values) (first (list-ref ids-and-values 2)))
                          (db-index-read (db-index-select txn index (tail id-and-values)) 5)))))))))))))

  (define-test (db-record-index-select env)
    (let (index-fields (list 0 4))
      (test-helper-type-create-1 env
        (l (type-name type-fields type)
          (test-helper-records-create-2 2 env
            type
            (l (ids-and-values)
              (let (index (db-index-create env type index-fields))
                (db-txn-call-write env
                  (l (txn)
                    (let*
                      ( (id-and-values (first ids-and-values))
                        (records
                          (map (l (a) (db-record->vector type a))
                            (db-record-index-read
                              (db-record-index-select txn index (tail id-and-values)) 5))))
                      (and (not (null? records)) (every vector? records))))))))))))

  (define-test (db-record-delete env) "and db-record-delete-type"
    (let (index-fields (list 0 4))
      (test-helper-type-create-1 env
        (l (type-name type-fields type)
          (assert-and
            (assert-true "record-delete"
              (test-helper-records-create-2 common-element-count env
                type
                (l (ids-and-values)
                  (let
                    ( (ids (map first ids-and-values))
                      (index (db-index-create env type index-fields)))
                    (db-txn-call-write env
                      (l (txn)
                        (and (not (null? (db-record-get txn ids)))
                          (begin (db-record-delete txn ids) (null? (db-record-get txn ids))
                            (begin (db-record-delete-type txn (db-type-id type)) #t)))))))))
            (assert-true "record-delete-type"
              (test-helper-records-create-2 common-element-count env
                type
                (l (ids-and-values)
                  (let (ids (map first ids-and-values))
                    (db-txn-call-write env
                      (l (txn)
                        (and (not (null? (db-record-get txn ids)))
                          (begin (db-record-delete-type txn (db-type-id type))
                            (null? (db-record-get txn ids)))))))))))))))

  (define-test (db-relation-delete env)
    (test-helper-relations-create-1 env
      (l (left right label)
        (let* ((count (apply + (map length (list left right label)))) (ordinal (list 0 count)))
          (db-txn-call-write env
            (l (txn)
              (and
                (not
                  (null?
                    (db-relation-read (db-relation-select txn left right label ordinal) (* 2 count))))
                (begin (db-relation-delete txn left right label ordinal)
                  (null?
                    (db-relation-read (db-relation-select txn left right label ordinal) (* 2 count)))))))))))

  (define-procedure-tests tests (db-relation-delete)
    (db-record-delete) (db-record-index-select)
    (db-record-update) (db-index-select)
    (db-record-create) (db-record-select)
    (db-statistics) (db-record-virtual)
    (db-other) (db-relation-ensure) (db-relation-select) (db-type) (db-index) (db-env) (db-txn))

  (l (settings)
    (let* ((test-runs 1) (settings (test-helper-db-default-test-settings settings)))
      (test-helper-delete-database-files)
      (apply append (map-integers test-runs (l (n) (test-execute-procedures settings tests)))))))
