(in-package #:rpc-backend-stdio/tests)

(defun %echo (method params)
  (cond
    ((equal method "echo") params)
    ((equal method "sum") (+ (elt params 0) (elt params 1)))
    (t (error 'rpc-protocol:rpc-method-not-found))))

#+sbcl
(defun %pipe ()
  (multiple-value-bind (r w) (sb-unix:unix-pipe)
    (values
     (sb-sys:make-fd-stream r :input t :element-type 'character
                            :external-format :utf-8 :buffering :line)
     (sb-sys:make-fd-stream w :output t :element-type 'character
                            :external-format :utf-8 :buffering :line))))

(deftest transport-class
  (ok (typep (rpc-backend-stdio:make-stdio-rpc-transport)
             'rpc-backend-stdio:stdio-rpc-transport)))

(deftest serve-one-message
  (let* ((req (rpc-protocol:encode-request "echo" "hi" :id 1))
         (in (make-string-input-stream (format nil "~a~%" req)))
         (out (make-string-output-stream))
         (tx (rpc-backend-stdio:make-stdio-rpc-transport :input in :output out)))
    (rpc-protocol:rpc-serve #'%echo :transport tx)
    (let ((msg (rpc-protocol:decode-message (get-output-stream-string out))))
      (ok (equal "hi" (gethash "result" msg)))
      (ok (= 1 (gethash "id" msg))))))

(deftest serve-notification-no-response
  (let* ((req (rpc-protocol:encode-notification "echo" "x"))
         (in (make-string-input-stream (format nil "~a~%" req)))
         (out (make-string-output-stream))
         (tx (rpc-backend-stdio:make-stdio-rpc-transport :input in :output out)))
    (rpc-protocol:rpc-serve #'%echo :transport tx)
    (ok (equal "" (get-output-stream-string out)))))

(deftest serve-method-not-found
  (let* ((req (rpc-protocol:encode-request "nope" nil :id 3))
         (in (make-string-input-stream (format nil "~a~%" req)))
         (out (make-string-output-stream))
         (tx (rpc-backend-stdio:make-stdio-rpc-transport :input in :output out)))
    (rpc-protocol:rpc-serve #'%echo :transport tx)
    (let ((msg (rpc-protocol:decode-message (get-output-stream-string out))))
      (ok (gethash "error" msg))
      (ok (= rpc-protocol:+method-not-found+
             (gethash "code" (gethash "error" msg)))))))

#+sbcl
(deftest call-over-pipes
  (multiple-value-bind (client-in server-out) (%pipe)
    (multiple-value-bind (server-in client-out) (%pipe)
      (let ((server (rpc-backend-stdio:make-stdio-rpc-transport
                     :input server-in :output server-out))
            (client (rpc-backend-stdio:make-stdio-rpc-transport
                     :input client-in :output client-out))
            (thread nil))
        (unwind-protect
             (progn
               (setf thread
                     (bt:make-thread
                      (lambda ()
                        (rpc-protocol:rpc-serve #'%echo :transport server))
                      :name "rpc-stdio-serve"))
               (ok (equal "hi" (rpc-protocol:rpc-call "echo" "hi" :transport client :id 1)))
               (ok (= 3 (rpc-protocol:rpc-call "sum" #(1 2) :transport client :id 2)))
               (ok (signals (rpc-protocol:rpc-call "nope" nil :transport client :id 3)
                            'rpc-protocol:rpc-error))
               (ok (rpc-protocol:rpc-notify "echo" "n" :transport client)))
          (ignore-errors (close client-out))
          (when thread
            (bt:join-thread thread))
          (ignore-errors (close client-in))
          (ignore-errors (close server-in))
          (ignore-errors (close server-out)))))))
