(in-package #:rpc-backend-stdio/tests)

;;; 0.4.1: :BARRIER-METHODS (egao1980/cl-stack-code#45). Under :CONCURRENT
;;; the messages read after a barrier request wait until its answer was
;;; written. The peer below pipelines everything (writes without waiting for
;;; an answer), as a replay harness or an eager client does. Every read has a
;;; deadline, so a deadlock fails a test instead of hanging the suite.

(defun %message-within (in &optional (seconds 10))
  "The next line of IN decoded, or NIL when none came within SECONDS (or EOF)."
  (let* ((line nil)
         (thread (bt2:make-thread (lambda () (setf line (read-line in nil nil)))
                                  :name "barrier-test read")))
    (and (%join thread seconds) line (rpc-protocol:decode-message line))))

(defun %messages-within (in n &optional (seconds 10))
  "N decoded lines of IN (fewer when one did not come in time)."
  (loop repeat n
        for m = (%message-within in seconds)
        while m collect m))

(defun %ids (messages)
  (mapcar (lambda (m) (gethash "id" m)) messages))

(defmacro with-barrier-peer ((client peer-in peer-out &key (dispatch :concurrent)
                                                           (barrier-methods ''("init"))
                                                           on-request on-notification)
                             &body body)
  `(with-raw-peer (,client ,peer-in ,peer-out
                   :dispatch ,dispatch :barrier-methods ,barrier-methods
                   :on-request ,on-request :on-notification ,on-notification)
     ,@body))

(deftest barrier-holds-pipelined-requests-and-notifications
  (let ((initialized nil) (lock (bt2:make-lock)) (notes '()))
    (with-barrier-peer (client peer-in peer-out
                        :on-request (lambda (m p)
                                      (declare (ignore p))
                                      (cond ((equal m "init")
                                             (sleep 0.3) ; a slow handshake
                                             (setf initialized t)
                                             "ready")
                                            (t (if initialized "after init" "before init"))))
                        :on-notification (lambda (m p)
                                           (declare (ignore m p))
                                           (bt2:with-lock-held (lock)
                                             (push (if initialized :after :before) notes))))
      (ok (equal '("init") (rpc-backend-stdio:stdio-rpc-transport-barrier-methods client)))
      ;; Written back to back: nothing waits for the init answer.
      (%peer-send peer-out (rpc-protocol:encode-request "init" nil :id 1))
      (%peer-send peer-out (rpc-protocol:encode-request "work" nil :id 2))
      (%peer-send peer-out (%notification "note"))
      (%peer-send peer-out (rpc-protocol:encode-request "work" nil :id 3))
      (let ((answers (%messages-within peer-in 3)))
        (ok (= 3 (length answers)) "every request is answered")
        (ok (eql 1 (first (%ids answers))) (format nil "the barrier's answer first: ~s" (%ids answers)))
        (ok (equal '(2 3) (sort (copy-list (rest (%ids answers))) #'<)))
        (ok (every (lambda (m) (equal "after init" (gethash "result" m))) (rest answers))
            "the requests read after it ran after it"))
      (ok (%wait-until (lambda () (bt2:with-lock-held (lock) notes)) :timeout 5))
      (ok (equal '(:after) (bt2:with-lock-held (lock) notes)) "and the notification"))))

(deftest barrier-released-requests-still-run-concurrently
  (let* ((lock (bt2:make-lock)) (inside 0) (peak 0))
    (with-barrier-peer (client peer-in peer-out
                        :on-request (lambda (m p)
                                      (declare (ignore p))
                                      (if (equal m "init")
                                          (progn (sleep 0.2) "ready")
                                          (progn
                                            (bt2:with-lock-held (lock)
                                              (incf inside) (setf peak (max peak inside)))
                                            ;; both wait until they run at once
                                            (%wait-until (lambda () (bt2:with-lock-held (lock) (= peak 2)))
                                                         :timeout 5)
                                            (bt2:with-lock-held (lock) (decf inside))
                                            "done"))))
      (%peer-send peer-out (rpc-protocol:encode-request "init" nil :id 1))
      (%peer-send peer-out (rpc-protocol:encode-request "work" nil :id 2))
      (%peer-send peer-out (rpc-protocol:encode-request "work" nil :id 3))
      (let ((answers (%messages-within peer-in 3)))
        (ok (eql 1 (first (%ids answers))))
        (ok (= 3 (length answers))))
      (ok (= 2 peak) "the held requests run concurrently once released"))))

(deftest barrier-handler-may-call-the-peer
  ;; Responses are routed while messages are held: the barrier's own call back
  ;; to the peer is answered.
  (let ((client-ref nil))
    (with-barrier-peer (client peer-in peer-out
                        :on-request (lambda (m p)
                                      (declare (ignore p))
                                      (if (equal m "init")
                                          (list "init saw"
                                                (rpc-protocol:rpc-call "whoami" nil
                                                                       :transport client-ref
                                                                       :timeout 5))
                                          "work done")))
      (setf client-ref client)
      (%peer-send peer-out (rpc-protocol:encode-request "init" nil :id "i"))
      (%peer-send peer-out (rpc-protocol:encode-request "work" nil :id "w"))
      (let ((call (%message-within peer-in)))
        (ok (and call (equal "whoami" (gethash "method" call))) "the barrier calls the peer")
        (when call
          (%peer-send peer-out (%response (gethash "id" call) "the peer"))))
      (let ((answers (%messages-within peer-in 2)))
        (ok (equal '("i" "w") (%ids answers)) (format nil "~s" (%ids answers)))
        (ok (equalp #("init saw" "the peer") (gethash "result" (first answers))))
        (ok (equal "work done" (gethash "result" (second answers))))))))

(deftest barrier-a-held-barrier-holds-again
  (let ((lock (bt2:make-lock)) (order '()))
    (with-barrier-peer (client peer-in peer-out
                        :on-request (lambda (m p)
                                      (when (equal m "init") (sleep 0.2))
                                      (bt2:with-lock-held (lock) (push p order))
                                      p))
      (loop for (method id) in '(("init" 1) ("work" 2) ("init" 3) ("work" 4))
            do (%peer-send peer-out (rpc-protocol:encode-request method id :id id)))
      (let ((ids (%ids (%messages-within peer-in 4))))
        (ok (= 4 (length ids)) (format nil "~s" ids))
        (ok (eql 1 (first ids)) "the first barrier first")
        (ok (and (member 3 ids) (member 4 ids) (< (position 3 ids) (position 4 ids)))
            "the request after the second barrier after its answer"))
      (ok (equal '(1) (last (bt2:with-lock-held (lock) order))) "the first barrier ran first"))))

(deftest barrier-wait-closed-waits-for-held-messages
  (with-barrier-peer (client peer-in peer-out
                      :on-request (lambda (m p)
                                    (declare (ignore p))
                                    (when (equal m "init") (sleep 0.3))
                                    m))
    (%peer-send peer-out (rpc-protocol:encode-request "init" nil :id 1))
    (%peer-send peer-out (rpc-protocol:encode-request "work" nil :id 2))
    (close peer-out)                    ; EOF while "work" is held
    (ok (rpc-backend-stdio:stdio-rpc-transport-wait-closed client :timeout 10)
        "closed, nothing running, nothing held")
    (ok (equal '(1 2) (%ids (%messages-within peer-in 2 2)))
        "the held request was handled before WAIT-CLOSED returned")))

(deftest barrier-close-drops-held-messages
  (let ((ran '()) (lock (bt2:make-lock)))
    (with-barrier-peer (client peer-in peer-out
                        :on-request (lambda (m p)
                                      (declare (ignore p))
                                      (bt2:with-lock-held (lock) (push m ran))
                                      (when (equal m "init") (sleep 0.5))
                                      m))
      (%peer-send peer-out (rpc-protocol:encode-request "init" nil :id 1))
      (%peer-send peer-out (rpc-protocol:encode-request "work" nil :id 2))
      (ok (%wait-until (lambda () (bt2:with-lock-held (lock) ran)) :timeout 5) "init runs")
      (sleep 0.05)
      (rpc-backend-stdio:close-stdio-rpc-transport client)
      (sleep 0.6)
      (ok (equal '("init") (bt2:with-lock-held (lock) ran))
          "the held request never started after the close"))))

(deftest-parametrize barrier-in-order-modes-unchanged
    ((dispatch) :ids ("reader" "serial") (:reader) (:serial))
  (with-barrier-peer (client peer-in peer-out
                      :dispatch dispatch
                      :on-request (lambda (m p) (declare (ignore m)) p))
    (loop for (method id) in '(("init" 1) ("work" 2) ("work" 3))
          do (%peer-send peer-out (rpc-protocol:encode-request method id :id id)))
    (ok (equal '(1 2 3) (%ids (%messages-within peer-in 3))) "arrival order, as before")))

(deftest barrier-methods-validation
  (ok (signals (rpc-backend-stdio:make-stdio-rpc-transport :barrier-methods "init") 'type-error))
  (ok (signals (rpc-backend-stdio:make-stdio-rpc-transport :barrier-methods '(:init)) 'error))
  (let ((tx (rpc-backend-stdio:make-stdio-rpc-transport)))
    (ok (null (rpc-backend-stdio:stdio-rpc-transport-barrier-methods tx)) "none by default")))
