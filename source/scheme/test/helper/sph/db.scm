(library (test helper sph db)
  (export
    test-helper-db-database-root
    test-helper-db-default-test-settings
    test-helper-delete-database-files
    test-helper-field-data
    test-helper-records-create-1
    test-helper-relations-create-1
    test-helper-type-create-1)
  (import
    (guile)
    (rnrs bytevectors)
    (sph)
    (sph alist)
    (sph char-set-vector)
    (sph db)
    (sph list)
    (sph one)
    (sph random-data)
    (only (rnrs base) set!))

  (define test-helper-db-database-root "/tmp/test/sph-db")
  (define (delete-file-if-exists a) (and (file-exists? a) (delete-file a)))

  (define* (test-helper-field-data type size)
    "should perhaps not be random to make it easier to find errors.
     if an error occurs with random data, the next test call would produce the same data"
    (case type
      ((int) (first (bytevector->sint-list (random-bytevector size) (endianness little) size)))
      ((uint) (first (bytevector->uint-list (random-bytevector size) (endianness little) size)))
      ((string) (random-string size char-set-vector:hex-digit))
      ((binary) (random-bytevector size))
      ((float) (/ (random size 1) 1.1))))

  (define (test-helper-type-create-1 env c)
    (let*
      ( (name "type-1")
        (fields
          (list-q
            ("field-1" . int64f)
            ("field-2" . uint8f)
            ("field-4" . binary256f)
            ("field-5" . float64f)
            ("field-3" . string8)
            ))
        (type (db-type-create env name fields)))
      (c name fields type)))

  ; todo: create type with all field types and test create/read

  (define (test-helper-records-create-1 count env type-1 c)
    (let
      (values
        (map-integers count
          (l (a)
            (let
              (a
                (compact
                  (list (and (random-boolean) (pair 0 (test-helper-field-data (q int) 8)))
                    (and (random-boolean) (pair 1 (test-helper-field-data (q uint) 1)))
                    (and (random-boolean) (pair 4 (test-helper-field-data (q string) 255)))
                    (and (random-boolean) (pair 3 (test-helper-field-data (q float) 8)))
                    (and (random-boolean) (pair 2 (test-helper-field-data (q binary) 32))))))
              (if (null? a) (list (pair 1 (test-helper-field-data (q uint) 1))) a)))))
      (apply c
        (db-txn-call-write env
          (l (txn)
            (let (ids (map (l (a) (db-record-create txn type-1 a)) values)) (list values ids)))))))

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
          (procedure-append-ignore-result existing-procedure-wrap procedure-wrap) procedure-wrap)))))
