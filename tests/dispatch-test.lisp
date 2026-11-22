(in-package #:rpc-backend-stdio/tests)

;;; 0.3.0: request dispatch off the reader thread (:DISPATCH :SERIAL /
;;; :CONCURRENT), calls back to the peer from inside a handler, notification
;;; order, shutdown with handlers in flight, error mapping, WAIT-CLOSED.
;;; Helpers (%OBJ, %WAIT-UNTIL, %JOIN, WITH-RAW-PEER, %PEER-SEND, …) are in
;;; peer-test.lisp. Every wait has a deadline, so a deadlock fails a test
;;; instead of hanging the suite.

(defmacro with-peers ((a b &key a-request b-request a-notification b-notification
                             (dispatch :serial))
                      &body body)
  "A and B: two client-mode transports over two pipes, each the other's peer."
  (let ((a-in (gensym)) (a-out (gensym)) (b-in (gensym)) (b-out (gensym)) (d (gensym)))
    `(let ((,d ,dispatch))
       (multiple-value-bind (,a-in ,b-out) (rpc-backend-stdio:make-stdio-pipe)
         (multiple-value-bind (,b-in ,a-out) (rpc-backend-stdio:make-stdio-pipe)
           (let* ((,a (rpc-backend-stdio:make-stdio-rpc-transport
                       :input ,a-in :output ,a-out :dispatch ,d
                       :on-request ,a-request :on-notification ,a-notification))
                  (,b (rpc-backend-stdio:make-stdio-rpc-transport
                       :input ,b-in :output ,b-out :dispatch ,d
                       :on-request ,b-request :on-notification ,b-notification)))
             (unwind-protect (progn ,@body)
               (rpc-backend-stdio:close-stdio-rpc-transport ,a)
               (rpc-backend-stdio:close-stdio-rpc-transport ,b))))))))

(defun %call-outcome (method params transport &key (timeout 10))
  "RPC-CALL → its value, or the RPC-ERROR it signalled."
  (handler-case (rpc-protocol:rpc-call method params :transport transport :timeout timeout)
    (rpc-protocol:rpc-error (e) e)))

(defun %elapsed-since (start)
  (/ (- (get-internal-real-time) start) internal-time-units-per-second))

(deftest dispatch-defaults-and-validation
  (multiple-value-bind (server client) (rpc-backend-stdio:make-stdio-rpc-pair)
    (ok (eq :reader (rpc-backend-stdio:stdio-rpc-transport-dispatch client)) "0.2.0 behaviour by default")
    (ok (eq :reader (rpc-backend-stdio:stdio-rpc-transport-dispatch server)))
    (rpc-backend-stdio:close-stdio-rpc-transport client)
    (rpc-backend-stdio:close-stdio-rpc-transport server))
  (ok (signals (rpc-backend-stdio:make-stdio-rpc-transport :dispatch :threads) 'type-error)))

;;; A calls B; B's handler calls A back and waits for the answer.

(deftest-parametrize nested-call-back
    ((dispatch) :ids ("serial" "concurrent") (:serial) (:concurrent))
  (let ((b nil))
    (with-peers (a b0
                 :dispatch dispatch
                 :a-request (lambda (method params)
                              (if (equal method "ping")
                                  (format nil "pong ~a" params)
                                  (error 'rpc-protocol:rpc-method-not-found)))
                 :b-request (lambda (method params)
                              (declare (ignore method))
                              ;; The call back to A, from inside B's handler.
                              (list "b saw" params
                                    (rpc-protocol:rpc-call "ping" params :transport b :timeout 5))))
      (setf b b0)
      (let* ((start (get-internal-real-time))
             (outcome (%call-outcome "ask" "x" a :timeout 10)))
        (ok (equalp #("b saw" "x" "pong x") outcome) (format nil "~s" outcome))
        (ok (< (%elapsed-since start) 5) "completes well inside the timeout"))
      (ok (equalp #("b saw" "y" "pong y") (%call-outcome "ask" "y" a))
          "the transports keep working"))))

(deftest nested-call-back-two-deep-concurrent
  ;; A → B → A → B: each handler calls the other side again until depth 0.
  (let ((a nil) (b nil))
    (flet ((handler (self-ref)
             (lambda (method params)
               (declare (ignore method))
               (if (zerop params)
                   "bottom"
                   (format nil "~a<~a" params
                           (rpc-protocol:rpc-call "down" (1- params)
                                                  :transport (funcall self-ref) :timeout 5))))))
      (with-peers (a0 b0 :dispatch :concurrent
                         :a-request (handler (lambda () a))
                         :b-request (handler (lambda () b)))
        (setf a a0 b b0)
        (ok (equal "4<3<2<1<bottom" (%call-outcome "down" 4 a :timeout 10))
            "four nested requests across both peers")))))

(deftest reader-dispatch-call-back-fails-fast
  ;; 0.2.0's mode: a call from the reader thread cannot be answered, so it
  ;; signals at once and the handler's error becomes the response.
  (let ((b nil))
    (with-peers (a b0 :dispatch :reader
                      :a-request (lambda (m p) (declare (ignore m)) p)
                      :b-request (lambda (m p) (declare (ignore m))
                                   (rpc-protocol:rpc-call "ping" p :transport b)))
      (setf b b0)
      (let* ((start (get-internal-real-time))
             (outcome (%call-outcome "ask" 1 a :timeout 10)))
        (ok (typep outcome 'rpc-protocol:rpc-error))
        (ok (and (typep outcome 'rpc-protocol:rpc-error)
                 (eql rpc-protocol:+internal-error+ (rpc-protocol:rpc-error-code outcome))))
        (ok (and (typep outcome 'rpc-protocol:rpc-error)
                 (search "deadlock" (princ-to-string outcome))))
        (ok (< (%elapsed-since start) 5) "no deadlock, no timeout")))))

;;; Concurrency.

(deftest concurrent-requests-overlap
  (let* ((lock (bt2:make-lock)) (inside 0) (peak 0) (n 4))
    (with-raw-peer (client peer-in peer-out
                    :dispatch :concurrent
                    :on-request (lambda (m p)
                                  (declare (ignore m))
                                  (bt2:with-lock-held (lock)
                                    (incf inside) (setf peak (max peak inside)))
                                  ;; Every handler waits until all N run at once.
                                  (%wait-until (lambda () (bt2:with-lock-held (lock) (= peak n)))
                                               :timeout 5)
                                  (bt2:with-lock-held (lock) (decf inside))
                                  (* 10 p)))
      (loop for i below n
            do (%peer-send peer-out (rpc-protocol:encode-request "work" i :id i)))
      (let ((answers (loop repeat n collect (rpc-protocol:decode-message (read-line peer-in)))))
        (ok (= n peak) (format nil "~a handlers ran at once" peak))
        (ok (equal '(0 10 20 30)
                   (sort (mapcar (lambda (r) (gethash "result" r)) answers) #'<)))
        (ok (every (lambda (r) (eql (* 10 (gethash "id" r)) (gethash "result" r))) answers)
            "each answer carries its own id")))))

(deftest serial-requests-one-at-a-time-in-order
  (let* ((lock (bt2:make-lock)) (inside 0) (peak 0) (order '()))
    (with-raw-peer (client peer-in peer-out
                    :dispatch :serial
                    :on-request (lambda (m p)
                                  (declare (ignore m))
                                  (bt2:with-lock-held (lock)
                                    (incf inside) (setf peak (max peak inside)) (push p order))
                                  (sleep 0.05)
                                  (bt2:with-lock-held (lock) (decf inside))
                                  p))
      (loop for i below 5
            do (%peer-send peer-out (rpc-protocol:encode-request "work" i :id i)))
      (let ((ids (loop repeat 5
                       collect (gethash "id" (rpc-protocol:decode-message (read-line peer-in))))))
        (ok (= 1 peak) "never two at once")
        (ok (equal '(0 1 2 3 4) (reverse order)) "in arrival order")
        (ok (equal '(0 1 2 3 4) ids) "answers in order")))))

(deftest concurrent-requests-from-many-callers
  ;; Several threads of A call B at once; B answers concurrently.
  (with-peers (a b :dispatch :concurrent
                   :a-request (lambda (m p) (declare (ignore m)) p)
                   :b-request (lambda (m p) (declare (ignore m)) (sleep (random 0.05)) (* p p)))
    (let* ((results (make-array 8 :initial-element nil))
           (threads (loop for i below 8
                          collect (let ((i i))
                                    (bt2:make-thread
                                     (lambda () (setf (aref results i) (%call-outcome "sq" i a))))))))
      (ok (every #'%join threads))
      (ok (equalp #(0 1 4 9 16 25 36 49) results)))))

;;; Notifications.

(deftest-parametrize notification-order-preserved
    ((dispatch) :ids ("reader" "serial" "concurrent") (:reader) (:serial) (:concurrent))
  (let* ((lock (bt2:make-lock)) (seen '()))
    (with-raw-peer (client peer-in peer-out
                    :dispatch dispatch
                    :on-notification (lambda (m p)
                                       (declare (ignore m))
                                       (when (zerop (mod p 7)) (sleep 0.01))
                                       (bt2:with-lock-held (lock) (push p seen)))
                    :on-request (lambda (m p) (declare (ignore m)) (sleep 0.1) p))
      (loop for i below 60
            do (%peer-send peer-out (%notification "session/update" i))
               (when (zerop (mod i 20))
                 (%peer-send peer-out (rpc-protocol:encode-request "slow" i :id i))))
      (ok (%wait-until (lambda () (bt2:with-lock-held (lock) (= 60 (length seen)))) :timeout 10))
      (ok (equal (loop for i below 60 collect i) (reverse seen)) "arrival order, one at a time")
      (ok (= 3 (loop repeat 3 count (read-line peer-in nil nil))) "the requests were answered"))))

(deftest concurrent-notification-seen-while-a-request-runs
  ;; ACP: session/cancel arrives while session/prompt still runs.
  (let* ((cancelled nil))
    (with-raw-peer (client peer-in peer-out
                    :dispatch :concurrent
                    :on-notification (lambda (m p) (declare (ignore p))
                                       (when (equal m "session/cancel") (setf cancelled t)))
                    :on-request (lambda (m p) (declare (ignore m p))
                                  (if (%wait-until (lambda () cancelled) :timeout 5)
                                      "cancelled"
                                      "never saw the cancel")))
      (%peer-send peer-out (rpc-protocol:encode-request "session/prompt" nil :id 1))
      (sleep 0.1)
      (%peer-send peer-out (%notification "session/cancel"))
      (let ((answer (rpc-protocol:decode-message (read-line peer-in))))
        (ok (equal "cancelled" (gethash "result" answer)))))))

;;; Shutdown.

(deftest-parametrize close-with-handlers-in-flight
    ((dispatch) :ids ("serial" "concurrent") (:serial) (:concurrent))
  (multiple-value-bind (a-in peer-out) (rpc-backend-stdio:make-stdio-pipe)
    (multiple-value-bind (peer-in a-out) (rpc-backend-stdio:make-stdio-pipe)
      (let* ((lock (bt2:make-lock))
             (callback-outcome nil) (handler-threads '()) (ran '())
             (client nil))
        (setf client
              (rpc-backend-stdio:make-stdio-rpc-transport
               :input a-in :output a-out :dispatch dispatch
               :on-request (lambda (m p)
                             (declare (ignore p))
                             (bt2:with-lock-held (lock)
                               (push (bt2:current-thread) handler-threads)
                               (push m ran))
                             (cond ((equal m "call-back")
                                    ;; The peer never answers this.
                                    (setf callback-outcome
                                          (%call-outcome "never" nil client :timeout nil))
                                    "unreachable")
                                   ((equal m "busy") (sleep 60) "slept")
                                   (t m)))))
        (%peer-send peer-out (rpc-protocol:encode-request "call-back" nil :id 1))
        (when (eq dispatch :concurrent)
          (%peer-send peer-out (rpc-protocol:encode-request "busy" nil :id 2)))
        (%peer-send peer-out (rpc-protocol:encode-request "queued" nil :id 3))
        (ok (read-line peer-in) "the handler's call back reached the peer")
        (ok (%wait-until (lambda () (bt2:with-lock-held (lock)
                                      (>= (length ran) (if (eq dispatch :concurrent) 3 1))))
                         :timeout 5))
        (let ((start (get-internal-real-time)))
          (rpc-backend-stdio:close-stdio-rpc-transport client)
          (ok (< (%elapsed-since start) 8) "close returns while handlers were in flight"))
        (ok (typep callback-outcome 'rpc-protocol:rpc-error)
            "the handler's pending call signalled RPC-ERROR")
        (ok (and (typep callback-outcome 'rpc-protocol:rpc-error)
                 (search "peer closed" (princ-to-string callback-outcome))))
        (ok (notany #'bt2:thread-alive-p (bt2:with-lock-held (lock) handler-threads))
            "every handler thread ended")
        (when (eq dispatch :serial)
          (ok (equal '("call-back") ran) "a queued request that had not started is dropped"))
        (ok (rpc-backend-stdio:stdio-rpc-transport-wait-closed client :timeout 2))
        (ignore-errors (close peer-out))
        (ignore-errors (close peer-in))))))

(deftest-parametrize peer-eof-drains-the-queue
    ((dispatch) :ids ("serial" "concurrent") (:serial) (:concurrent))
  ;; Messages read before the peer's EOF are still handled.
  (let* ((lock (bt2:make-lock)) (seen '()))
    (with-raw-peer (client peer-in peer-out
                    :dispatch dispatch
                    :on-notification (lambda (m p) (declare (ignore m))
                                       (sleep 0.02)
                                       (bt2:with-lock-held (lock) (push p seen))))
      (loop for i below 10 do (%peer-send peer-out (%notification "n" i)))
      (close peer-out)
      (ok (rpc-backend-stdio:stdio-rpc-transport-wait-closed client :timeout 5)
          "WAIT-CLOSED returns once EOF came and the handlers finished")
      (ok (equal (loop for i below 10 collect i) (reverse seen))))))

(deftest wait-closed-times-out-while-open
  (with-raw-peer (client peer-in peer-out :dispatch :concurrent
                                          :on-notification (lambda (m p) (declare (ignore m p))))
    (let ((start (get-internal-real-time)))
      (ok (null (rpc-backend-stdio:stdio-rpc-transport-wait-closed client :timeout 0.3)))
      (ok (< (%elapsed-since start) 3)))
    (let ((waiter (bt2:make-thread
                   (lambda () (rpc-backend-stdio:stdio-rpc-transport-wait-closed client :timeout 10)))))
      (sleep 0.1)
      (close peer-out)
      (ok (%join waiter 5) "the waiter returns at the peer's EOF"))))

;;; Errors and the request id.

(deftest-parametrize dispatch-error-mapping
    ((dispatch) :ids ("reader" "serial" "concurrent") (:reader) (:serial) (:concurrent))
  (let ((ids (make-hash-table :test 'equal)) (lock (bt2:make-lock)))
    (with-raw-peer (client peer-in peer-out
                    :dispatch dispatch
                    :on-request (lambda (m p)
                                  (declare (ignore p))
                                  (bt2:with-lock-held (lock)
                                    (setf (gethash m ids) rpc-backend-stdio:*stdio-rpc-request-id*))
                                  (cond ((equal m "rpc-fail")
                                         (error 'rpc-protocol:rpc-error :code -32050 :message "no"
                                                                        :data (%obj "why" "because")))
                                        ((equal m "lisp-fail") (error "plain ~a" "boom"))
                                        (t "ok"))))
      (flet ((ask (method id)
               (%peer-send peer-out (rpc-protocol:encode-request method nil :id id))
               (rpc-protocol:decode-message (read-line peer-in))))
        (let ((r (ask "rpc-fail" "r1")))
          (ok (equal "r1" (gethash "id" r)))
          (ok (eql -32050 (%error-code r)))
          (ok (equal "no" (gethash "message" (gethash "error" r))))
          (ok (equal "because" (gethash "why" (gethash "data" (gethash "error" r))))))
        (let ((r (ask "lisp-fail" 2)))
          (ok (eql 2 (gethash "id" r)))
          (ok (eql rpc-protocol:+internal-error+ (%error-code r)))
          (ok (search "plain boom" (gethash "message" (gethash "error" r)))))
        (let ((r (ask "fine" 3)))
          (ok (equal "ok" (gethash "result" r)) "the transport goes on after handler errors")))
      (ok (equal "r1" (gethash "rpc-fail" ids)) "*STDIO-RPC-REQUEST-ID* is the request's id")
      (ok (eql 3 (gethash "fine" ids))))))

(deftest-parametrize dispatch-without-on-request
    ((dispatch) :ids ("serial" "concurrent") (:serial) (:concurrent))
  (with-raw-peer (client peer-in peer-out :dispatch dispatch
                                          :on-notification (lambda (m p) (declare (ignore m p))))
    (%peer-send peer-out (rpc-protocol:encode-request "ask" nil :id 7))
    (let ((answer (rpc-protocol:decode-message (read-line peer-in))))
      (ok (eql 7 (gethash "id" answer)))
      (ok (eql rpc-protocol:+method-not-found+ (%error-code answer))))))
