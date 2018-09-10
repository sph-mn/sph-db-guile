(library (test helper sph db)
  (export
    test-helper-db-database-root
    test-helper-db-default-test-settings
    test-helper-delete-database-files
    test-helper-random-data
    test-helper-records-create-1
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

  (define* (test-helper-random-data type size)
    (case type
      ((int) (first (bytevector->sint-list (random-bytevector size) (endianness little) size)))
      ((uint) (first (bytevector->uint-list (random-bytevector size) (endianness little) size)))
      ((string) (random-string (random size 1) char-set-vector:hex-digit))
      ((binary) (string->utf8 (random-string (random size 1) char-set-vector:hex-digit)))
      ((float) (/ (random size 1) 1.1))))

  (define (test-helper-type-create-1 env c)
    (let*
      ( (name "type-1")
        (fields
          (list-q ("field-1" . int64) ("field-2" . uint8)
            ("field-4" . string512) ("field-5" . float64) ("field-3" . string)))
        (type (db-type-create env name fields)))
      (c name fields type)))

  (define (test-helper-records-create-1 env type-1 c)
    (let
      (values
        (map-integers 5
          (l (a)
            (let
              (a
                (compact
                  (list (and (random-boolean) (pair 0 (test-helper-random-data (q int) 8)))
                    (and (random-boolean) (pair 1 (test-helper-random-data (q uint) 1)))
                    (and (random-boolean) (pair 2 (test-helper-random-data (q string) 64)))
                    (and (random-boolean) (pair 3 (test-helper-random-data (q float) 8)))
                    (and (random-boolean) (pair 4 (test-helper-random-data (q string) 255))))))
              (if (null? a) (list (pair 1 (test-helper-random-data (q uint) 1))) a)))))
      (apply c
        (db-txn-call-write env
          (l (txn) (list values (map (l (a) (db-record-create txn type-1 a)) values)))))))

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
