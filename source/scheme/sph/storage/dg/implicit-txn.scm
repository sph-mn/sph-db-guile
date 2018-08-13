(library (sph storage dg implicit-txn)
  (export
    dg-delete*
    dg-exists?*
    dg-extern-create*
    dg-extern-data->id*
    dg-extern-id->data*
    dg-extern-update*
    dg-id-create*
    dg-identify*
    dg-index-errors-extern*
    dg-index-errors-intern*
    dg-index-errors-relation*
    dg-intern-data->id*
    dg-intern-ensure*
    dg-intern-id->data*
    dg-intern-update*
    dg-relation-delete*
    dg-relation-ensure*
    dg-relation-select-read*
    dg-statistics*)
  (import
    (sph)
    (sph storage dg)
    (only (guile) symbol-append eval-when))

  ;this module defines procedures corresponding to (sph storage dg) procedures.
  ;the new procedures do not take a transaction as the first argument and the transaction is automatically managed
  ;using dg-txn-call-read and dg-txn-call-write

  (define-syntax-rule (dg-with-txn proc dg-txn-call)
    (l a (dg-txn-call (l (txn) (apply proc txn a)))))

  (define-syntax-rule (dg-with-txn-read proc) (dg-with-txn proc dg-txn-call-read))
  (define-syntax-rule (dg-with-txn-write proc) (dg-with-txn proc dg-txn-call-write))

  (define-syntax-case (dg-define-with-txn with-txn proc-name ...) s
    (let
      ( (proc-name-datum (syntax->datum (syntax (proc-name ...))))
        (with-txn-datum (syntax->datum (syntax with-txn))))
      (datum->syntax s
        (pair (q begin)
          (map (l (a) (list (q define) (symbol-append a (q *)) (list with-txn-datum a)))
            proc-name-datum)))))

  (define-syntax-case (dg-define-with-txn-read a ...) s
    (datum->syntax s
      (qq (dg-define-with-txn dg-with-txn-read (unquote-splicing (syntax->datum (syntax (a ...))))))))

  (define-syntax-case (dg-define-with-txn-write a ...) s
    (datum->syntax s
      (qq
        (dg-define-with-txn dg-with-txn-write (unquote-splicing (syntax->datum (syntax (a ...))))))))

  (dg-define-with-txn-write dg-delete dg-extern-create
    dg-extern-update dg-id-create
    dg-intern-ensure dg-intern-update dg-relation-ensure dg-relation-delete)

  (dg-define-with-txn-read dg-exists? dg-extern-data->id
    dg-extern-id->data dg-identify
    dg-index-errors-extern dg-index-errors-intern
    dg-index-errors-relation dg-intern-data->id
    dg-intern-id->data dg-statistics dg-relation-select-read))
