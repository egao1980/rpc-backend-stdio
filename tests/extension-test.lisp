(in-package #:rpc-backend-stdio/tests)

;;; 0.3.2 (#16): :CLASS on the constructors and the STDIO-RPC-PEER-CLOSED
;;; condition. Helpers are in peer-test.lisp.

(defclass %sub-transport (rpc-backend-stdio:stdio-rpc-transport)
  ((tag :initform :sub :reader %sub-tag)))

(deftest class-initarg-on-transport
  (multiple-value-bind (in out) (rpc-backend-stdio:make-stdio-pipe)
    (declare (ignore out))
    (let ((tx (rpc-backend-stdio:make-stdio-rpc-transport :input in :class '%sub-transport)))
      (ok (typep tx '%sub-transport) "a symbol")
      (ok (eq :sub (%sub-tag tx))))
    (ok (typep (rpc-backend-stdio:make-stdio-rpc-transport :input in :class (find-class '%sub-transport))
               '%sub-transport)
        "a class object"))
  (ok (typep (rpc-backend-stdio:make-stdio-rpc-transport) 'rpc-backend-stdio:stdio-rpc-transport)
      "the default class")
  (ok (signals (rpc-backend-stdio:make-stdio-rpc-transport :class 'string) 'type-error)
      "not a transport class"))

(deftest class-initarg-on-pair
  (multiple-value-bind (server client)
      (rpc-backend-stdio:make-stdio-rpc-pair :class '%sub-transport
                                             :on-notification (lambda (m p) (declare (ignore m p))))
    (unwind-protect
         (let ((thread (bt2:make-thread
                        (lambda () (ignore-errors (rpc-protocol:rpc-serve (lambda (m p) (declare (ignore m)) p)
                                                                           :transport server)))
                        :name "class-pair-serve")))
           (ok (typep server '%sub-transport))
           (ok (typep client '%sub-transport))
           (ok (equal "hi" (rpc-protocol:rpc-call "echo" "hi" :transport client :timeout 5))
               "the subclass works as a transport")
           (rpc-backend-stdio:close-stdio-rpc-transport client)
           (ok (%join thread 5)))
      (rpc-backend-stdio:close-stdio-rpc-transport client))))

(deftest peer-closed-condition
  (with-raw-peer (client peer-in peer-out)
    (let* ((outcome nil)
           (thread (bt2:make-thread
                    (lambda ()
                      (setf outcome
                            (handler-case (progn (rpc-protocol:rpc-call "x" nil :transport client) :returned)
                              (rpc-protocol:rpc-error (e) e)))))))
      (read-line peer-in)
      (close peer-out)
      (ok (%join thread 5))
      (ok (typep outcome 'rpc-backend-stdio:stdio-rpc-peer-closed) "the waiting call")
      (ok (typep outcome 'rpc-protocol:rpc-error) "still an RPC-ERROR")
      (ok (and (typep outcome 'rpc-protocol:rpc-error)
               (eql rpc-protocol:+internal-error+ (rpc-protocol:rpc-error-code outcome)))
          "code -32603")
      (ok (search "stdio RPC peer closed" (princ-to-string outcome)) "the message is unchanged")
      (ok (signals (rpc-protocol:rpc-call "y" nil :transport client :timeout 1)
              'rpc-backend-stdio:stdio-rpc-peer-closed)
          "later calls"))))
