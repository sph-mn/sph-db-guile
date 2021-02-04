(define-module (test helper sph db))
(use-modules (rnrs bytevectors) (sph) (sph alist) (sph db) (sph list) (sph list other) (sph other))

(export test-helper-db-database-root test-helper-db-default-test-settings
  test-helper-delete-database-files test-helper-field-data
  test-helper-records-create-1 test-helper-records-create-2
  test-helper-relations-create-1 test-helper-type-create-1)

(define test-helper-db-database-root "/tmp/test/sph-db")
(define char-set-vector:hex-digit (list->vector (char-set->list char-set:hex-digit)))
(define (delete-file-if-exists a) (and (file-exists? a) (delete-file a)))

(define* (test-helper-field-data type size)
  "should perhaps not be random to make it easier to find errors.
   if an error occurs with random data, the next test call doesnt produce the same data"
  (case type
    ((int) (first (bytevector->sint-list (random-bytevector size) (endianness little) size)))
    ((uint) (first (bytevector->uint-list (random-bytevector size) (endianness little) size)))
    ((string) (random-string size char-set-vector:hex-digit))
    ((binary) (random-bytevector size))
    ((float) (/ (+ 1 (random size)) 1.1))))

(define (test-helper-type-create-1 env c)
  (let*
    ( (name "type-1")
      (fields
        (q (("field-0" . int64f) ("field-1" . uint8f)
            ("field-2" . binary256f) ("field-3" . float64f) ("field-4" . string8))))
      (type (db-type-create env name fields)))
    (c name fields type)))

"todo: create type with all field types and test create/read"

(define (test-helper-records-create-1 count env type-1 c) "records that use the full field size"
  (let
    (values
      (map-integers count
        (l (a)
          (let
            (a
              (compact
                (list (and (random-boolean) (pair 0 (test-helper-field-data (q int) 8)))
                  (and (random-boolean) (pair 1 (test-helper-field-data (q uint) 1)))
                  (and (random-boolean) (pair 2 (test-helper-field-data (q binary) 32)))
                  (and (random-boolean) (pair 3 (test-helper-field-data (q float) 8)))
                  (and (random-boolean) (pair 4 (test-helper-field-data (q string) 7))))))
            (randomise (if (null? a) (list (pair 1 (test-helper-field-data (q uint) 1))) a))))))
    (c
      (db-txn-call-write env
        (l (txn) (map (l (values) (pair (db-record-create txn type-1 values) values)) values))))))

(define (test-helper-records-create-2 count env type-1 c)
  "records that dont use a bigger size than what fits into indices"
  (let*
    ( (values-1
        (list (pair 0 -123) (pair 1 123)
          (pair 2 (uint-list->bytevector (list 1 2 3) (endianness little) 3)) (pair 3 1.23)
          (pair 4 "123")))
      (values
        (list values-1
          (list (pair 0 -45) (pair 1 45)
            (pair 2 (uint-list->bytevector (list 4 5 6) (endianness little) 3)) (pair 3 4.56)
            (pair 4 "456"))
          values-1)))
    (c
      (db-txn-call-write env
        (l (txn) (map (l (values) (pair (db-record-create txn type-1 values) values)) values))))))

(define (test-helper-relations-create-1 env c)
  (let ((left (list 1 2 3)) (right (list 1 2)) (label (list 7 8)))
    (apply c
      (db-txn-call-write env
        (l (txn) (and (db-relation-ensure txn left right label) (list left right label)))))))

(define (test-helper-delete-database-files)
  (each delete-file-if-exists
    (list (string-append test-helper-db-database-root "/data")
      (string-append test-helper-db-database-root "/data-lock"))))

(define (test-helper-db-default-test-settings settings)
  "add/append the default settings to the given test settings object.
   by default, the database is created before each procedure test.
   after each procedure test the database is deleted"
  (let
    ( (procedure-wrap
        (l (test-proc test-name)
          (l a
            (begin-first (db-use test-helper-db-database-root (l (env) (apply test-proc env a)))
              (test-helper-delete-database-files)))))
      (existing-procedure-wrap (alist-ref settings (q procedure-wrap))))
    (alist-set-multiple settings (q exception->key)
      #t (q procedure-wrap)
      (if existing-procedure-wrap
        (procedure-append-ignore-result existing-procedure-wrap procedure-wrap) procedure-wrap))))
