(in-package #:rpc-backend-stdio/tests)

;;; 0.2.0: in-memory pipes and pairs, the client's reader thread (responses
;;; by id, notifications, server requests, timeouts, a closed peer), and the
;;; server's answers to parse errors, invalid requests and batches. See the
;;; headers of src/backend.lisp and src/pipe.lisp.

(defun %obj (&rest kv)
  (let ((h (make-hash-table :test 'equal)))
    (loop for (k v) on kv by #'cddr do (setf (gethash k h) v))
    h))

(defun %wait-until (predicate &key (timeout 10))
  "Poll PREDICATE every 20 ms until it is true (→ its value) or TIMEOUT
seconds pass (→ NIL)."
  (let ((deadline (+ (get-internal-real-time) (* timeout internal-time-units-per-second))))
    (loop
      (let ((value (funcall predicate)))
        (when value (return value)))
      (when (> (get-internal-real-time) deadline) (return nil))
      (sleep 0.02))))

(defun %join (thread &optional (timeout 10))
  "Join THREAD when it finishes within TIMEOUT seconds. → T, or NIL (the
thread is left behind)."
  (when (%wait-until (lambda () (not (bt2:thread-alive-p thread))) :timeout timeout)
    (bt2:join-thread thread)
    t))

(defun %serve-lines (handler &rest lines)
  "RPC-SERVE HANDLER over LINES (string streams). → the decoded output lines."
  (let* ((in (make-string-input-stream (format nil "~{~a~%~}" lines)))
         (out (make-string-output-stream))
         (tx (rpc-backend-stdio:make-stdio-rpc-transport :input in :output out)))
    (rpc-protocol:rpc-serve handler :transport tx)
    (with-input-from-string (s (get-output-stream-string out))
      (loop for line = (read-line s nil) while line
            collect (rpc-protocol:decode-message line)))))

(defun %nth (n list)
  (and (listp list) (nth n list)))

(defun %null-id-p (message)
  "MESSAGE has an \"id\" key whose value is JSON null (:NULL or NIL, by the
json-protocol backend)."
  (and (hash-table-p message)
       (multiple-value-bind (value present) (gethash "id" message)
         (and present (member value '(:null nil)) t))))

(defun %error-code (message)
  (and (hash-table-p message)
       (hash-table-p (gethash "error" message))
       (gethash "code" (gethash "error" message))))

;;; Pipes.

(deftest pipe-round-trip
  (multiple-value-bind (in out) (rpc-backend-stdio:make-stdio-pipe)
    (ok (input-stream-p in))
    (ok (output-stream-p out))
    (ok (not (output-stream-p in)))
    (write-line "héllo" out)
    (write-string "wor" out)
    (write-string "ld" out)
    (terpri out)
    (finish-output out)
    (ok (equal "héllo" (read-line in)))
    (ok (equal "world" (read-line in)))
    (close out)
    (ok (eq :eof (read-line in nil :eof)) "EOF once the writer closed and the buffer is empty")
    (ok (not (open-stream-p out)))))

(deftest pipe-read-blocks-until-written
  (multiple-value-bind (in out) (rpc-backend-stdio:make-stdio-pipe)
    (let* ((got nil)
           (reader (bt2:make-thread (lambda () (setf got (read-line in nil :eof))))))
      (sleep 0.2)
      (ok (null got) "still waiting")
      (write-line "late" out)
      (finish-output out)
      (ok (%join reader))
      (ok (equal "late" got)))))

(deftest pipe-close-wakes-reader
  (multiple-value-bind (in out) (rpc-backend-stdio:make-stdio-pipe)
    (let* ((got nil)
           (reader (bt2:make-thread (lambda () (setf got (read-line in nil :eof))))))
      (sleep 0.2)
      (close out)
      (ok (%join reader) "the blocked reader returns")
      (ok (eq :eof got)))))

(deftest pipe-capacity-back-pressure
  (multiple-value-bind (in out) (rpc-backend-stdio:make-stdio-pipe :capacity 8)
    (let* ((done nil)
           (text (make-string 100 :initial-element #\z))
           (writer (bt2:make-thread (lambda () (write-line text out) (finish-output out)
                                      (setf done t)))))
      (sleep 0.2)
      (ok (null done) "the writer waits while 8 characters are unread")
      (ok (equal text (read-line in)) "everything arrives, in order")
      (ok (%join writer))
      (ok done))))

(deftest pipe-broken
  (multiple-value-bind (in out) (rpc-backend-stdio:make-stdio-pipe)
    (close in)
    (ok (signals (progn (write-line "x" out) (finish-output out)) 'stream-error))))

(deftest pipe-writes-from-threads-stay-whole
  (multiple-value-bind (in out) (rpc-backend-stdio:make-stdio-pipe :capacity 1024)
    (let* ((n 4) (m 50)
           (writers (loop for i below n
                          collect (let ((c (code-char (+ 97 i))))
                                    (bt2:make-thread
                                     (lambda ()
                                       (loop repeat m
                                             do (write-string (format nil "~a~%" (make-string 300 :initial-element c)) out))))))))
      (let ((lines (loop repeat (* n m) collect (read-line in))))
        (mapc #'%join writers)
        (ok (= (* n m) (length lines)))
        (ok (every (lambda (line)
                     (and (= 300 (length line))
                          (every (lambda (ch) (char= ch (char line 0))) line)))
                   lines)
            "no line mixes two writers")))))

;;; Pairs.

(defun %pair-handler (server)
  (lambda (method params)
    (cond ((equal method "echo") params)
          ((equal method "chatty")
           (rpc-protocol:rpc-notify "tick" 1 :transport server)
           (rpc-protocol:rpc-notify "tick" 2 :transport server)
           (rpc-protocol:rpc-notify "tick" 3 :transport server)
           params)
          ((equal method "fail")
           (error 'rpc-protocol:rpc-error :code -32042 :message "nope"
                                          :data (%obj "why" "because")))
          (t (error 'rpc-protocol:rpc-method-not-found)))))

(defmacro with-pair ((server client &rest keys) &body body)
  "SERVER serving %PAIR-HANDLER in a thread; CLIENT the other end. Closing the
client afterwards ends the server's loop."
  (let ((thread (gensym "THREAD")))
    `(multiple-value-bind (,server ,client) (rpc-backend-stdio:make-stdio-rpc-pair ,@keys)
       (let ((,thread (bt2:make-thread
                       (lambda ()
                         (ignore-errors
                          (rpc-protocol:rpc-serve (%pair-handler ,server) :transport ,server)))
                       :name "pair-serve")))
         (unwind-protect (progn ,@body)
           (rpc-backend-stdio:close-stdio-rpc-transport ,client)
           (%join ,thread 5))))))

(deftest pair-round-trip
  (with-pair (server client)
    (ok (typep server 'rpc-backend-stdio:stdio-rpc-transport))
    (ok (typep client 'rpc-backend-stdio:stdio-rpc-transport))
    (ok (equal "hi" (rpc-protocol:rpc-call "echo" "hi" :transport client :timeout 5)))
    (ok (equalp #(1 2) (rpc-protocol:rpc-call "echo" #(1 2) :transport client :timeout 5)))
    (ok (rpc-protocol:rpc-notify "echo" "ignored" :transport client))
    (ok (equal "again" (rpc-protocol:rpc-call "echo" "again" :transport client :timeout 5)))))

(deftest pair-close-ends-the-server
  (multiple-value-bind (server client) (rpc-backend-stdio:make-stdio-rpc-pair)
    (let ((thread (bt2:make-thread
                   (lambda () (rpc-protocol:rpc-serve (%pair-handler server) :transport server)))))
      (ok (equal "x" (rpc-protocol:rpc-call "echo" "x" :transport client :timeout 5)))
      (rpc-backend-stdio:close-stdio-rpc-transport client)
      (ok (%join thread 5) "RPC-SERVE returns at EOF"))))

(deftest pair-error-response
  (with-pair (server client)
    (let ((c (handler-case (progn (rpc-protocol:rpc-call "fail" nil :transport client :timeout 5) nil)
               (rpc-protocol:rpc-error (e) e))))
      (ok c)
      (ok (eql -32042 (rpc-protocol:rpc-error-code c)))
      (ok (equal "nope" (rpc-protocol:rpc-error-message c)))
      (ok (equal "because" (gethash "why" (rpc-protocol:rpc-error-data c)))))
    (ok (eql rpc-protocol:+method-not-found+
             (handler-case (progn (rpc-protocol:rpc-call "nope" nil :transport client :timeout 5) nil)
               (rpc-protocol:rpc-error (e) (rpc-protocol:rpc-error-code e)))))))

(deftest call-skips-interleaved-notifications
  (let* ((seen '())
         (lock (bt2:make-lock)))
    (with-pair (server client
                :on-notification (lambda (method params)
                                   (bt2:with-lock-held (lock) (push (list method params) seen))))
      (ok (equal "done" (rpc-protocol:rpc-call "chatty" "done" :transport client :timeout 5))
          "the response, not a notification, is the result")
      (ok (%wait-until (lambda () (= 3 (length seen)))))
      (ok (equal '(("tick" 1) ("tick" 2) ("tick" 3)) (reverse seen)) "in arrival order")
      (ok (equal "next" (rpc-protocol:rpc-call "echo" "next" :transport client :timeout 5))))))

(deftest call-drops-notifications-without-a-handler
  (with-pair (server client)
    (ok (equal "done" (rpc-protocol:rpc-call "chatty" "done" :transport client :timeout 5)))
    (ok (equal "ok" (rpc-protocol:rpc-call "echo" "ok" :transport client :timeout 5)))))

;;; The reader against a scripted peer (raw pipes).

(defmacro with-raw-peer ((client peer-in peer-out &rest keys) &body body)
  "CLIENT: a transport whose output is read from PEER-IN and whose input is
what is written to PEER-OUT (both pipe ends)."
  (let ((a-in (gensym)) (a-out (gensym)))
    `(multiple-value-bind (,a-in ,peer-out) (rpc-backend-stdio:make-stdio-pipe)
       (multiple-value-bind (,peer-in ,a-out) (rpc-backend-stdio:make-stdio-pipe)
         (declare (ignorable ,peer-in))
         (let ((,client (rpc-backend-stdio:make-stdio-rpc-transport :input ,a-in :output ,a-out ,@keys)))
           (unwind-protect (progn ,@body)
             (ignore-errors (close ,peer-out))
             (rpc-backend-stdio:close-stdio-rpc-transport ,client)))))))

(defun %peer-send (out line)
  (write-line line out)
  (finish-output out))

(defun %response (id result)
  (rpc-protocol:encode-response result :id id))

(defun %notification (method &optional params)
  (rpc-protocol:encode-notification method params))

(deftest calls-matched-by-id
  (with-raw-peer (client peer-in peer-out)
    (let* ((results (make-array 3 :initial-element nil))
           (threads (loop for i below 3
                          collect (let ((i i))
                                    (bt2:make-thread
                                     (lambda ()
                                       (setf (aref results i)
                                             (rpc-protocol:rpc-call "echo" i :transport client :timeout 10))))))))
      ;; Read the three requests, answer them in reverse order.
      (let ((requests (loop repeat 3 collect (rpc-protocol:decode-message (read-line peer-in)))))
        (ok (= 3 (length (remove-duplicates (mapcar (lambda (r) (gethash "id" r)) requests)
                                            :test #'equal)))
            "three distinct ids")
        (dolist (r (reverse requests))
          (%peer-send peer-out (%response (gethash "id" r) (* 10 (gethash "params" r))))))
      (mapc #'%join threads)
      (ok (equalp #(0 10 20) results) "each call got its own answer"))))

(deftest call-ids-count-from-one
  (with-raw-peer (client peer-in peer-out)
    (let ((thread (bt2:make-thread (lambda () (rpc-protocol:rpc-call "echo" "a" :transport client :timeout 10)))))
      (let ((r (rpc-protocol:decode-message (read-line peer-in))))
        (ok (eql 1 (gethash "id" r)))
        (ok (equal "2.0" (gethash "jsonrpc" r)))
        (ok (equal "echo" (gethash "method" r)))
        (%peer-send peer-out (%response 99 "stray"))
        (%peer-send peer-out (%response 1 "mine")))
      (ok (%join thread)))))

(deftest call-timeout
  (with-raw-peer (client peer-in peer-out)
    (let* ((start (get-internal-real-time))
           (c (handler-case (progn (rpc-protocol:rpc-call "slow" nil :transport client :timeout 0.3) nil)
                (rpc-backend-stdio:stdio-rpc-timeout (e) e)))
           (elapsed (/ (- (get-internal-real-time) start) internal-time-units-per-second)))
      (ok c "STDIO-RPC-TIMEOUT")
      (ok (typep c 'rpc-protocol:rpc-error))
      (ok (equal "slow" (and c (rpc-backend-stdio:stdio-rpc-timeout-method c))))
      (ok (< elapsed 3))
      ;; The late answer is dropped; the transport still works.
      (let ((r (rpc-protocol:decode-message (read-line peer-in))))
        (%peer-send peer-out (%response (gethash "id" r) "late")))
      (let ((thread (bt2:make-thread (lambda () (rpc-protocol:rpc-call "echo" 1 :transport client :timeout 10)))))
        (let ((r (rpc-protocol:decode-message (read-line peer-in))))
          (%peer-send peer-out (%response (gethash "id" r) "fresh")))
        (ok (%join thread))))))

(deftest peer-closed-wakes-waiting-calls
  (with-raw-peer (client peer-in peer-out)
    (let* ((outcome nil)
           (thread (bt2:make-thread
                    (lambda ()
                      (setf outcome
                            (handler-case (progn (rpc-protocol:rpc-call "x" nil :transport client) :returned)
                              (rpc-protocol:rpc-error (e) e)))))))
      (read-line peer-in)
      (close peer-out)
      (ok (%join thread 5) "the waiting call returns")
      (ok (typep outcome 'rpc-protocol:rpc-error))
      (ok (search "peer closed" (princ-to-string outcome)))
      (ok (signals (rpc-protocol:rpc-call "y" nil :transport client :timeout 1) 'rpc-protocol:rpc-error)
          "later calls fail at once"))))

(deftest notifications-before-any-call
  (let ((seen '()) (lock (bt2:make-lock)))
    (with-raw-peer (client peer-in peer-out
                    :on-notification (lambda (m p) (bt2:with-lock-held (lock) (push (cons m p) seen))))
      (%peer-send peer-out (%notification "hello" (%obj "n" 1)))
      (%peer-send peer-out (%notification "hello" (%obj "n" 2)))
      (ok (%wait-until (lambda () (= 2 (length seen))))
          "the reader starts with the transport when it has a handler")
      (ok (equal '(1 2) (mapcar (lambda (e) (gethash "n" (cdr e))) (reverse seen)))))))

(deftest notification-handler-errors-are-dropped
  (let ((count 0))
    (with-raw-peer (client peer-in peer-out
                    :on-notification (lambda (m p) (declare (ignore m p)) (incf count) (error "handler bug")))
      (%peer-send peer-out (%notification "a"))
      (%peer-send peer-out (%notification "b"))
      (ok (%wait-until (lambda () (= 2 count))) "reading goes on")
      (let ((thread (bt2:make-thread (lambda () (rpc-protocol:rpc-call "echo" 1 :transport client :timeout 5)))))
        (let ((r (rpc-protocol:decode-message (read-line peer-in))))
          (%peer-send peer-out (%response (gethash "id" r) 1)))
        (ok (%join thread))))))

(deftest server-requests-answered-by-on-request
  (with-raw-peer (client peer-in peer-out
                  :on-request (lambda (method params)
                                (cond ((equal method "add") (+ (elt params 0) (elt params 1)))
                                      (t (error 'rpc-protocol:rpc-error :code -32050 :message "no")))))
    (%peer-send peer-out (rpc-protocol:encode-request "add" #(2 3) :id "s1"))
    (let ((answer (rpc-protocol:decode-message (read-line peer-in))))
      (ok (equal "s1" (gethash "id" answer)))
      (ok (eql 5 (gethash "result" answer))))
    (%peer-send peer-out (rpc-protocol:encode-request "other" nil :id "s2"))
    (let ((answer (rpc-protocol:decode-message (read-line peer-in))))
      (ok (equal "s2" (gethash "id" answer)))
      (ok (eql -32050 (%error-code answer))))))

(deftest server-requests-without-a-handler
  (with-raw-peer (client peer-in peer-out :on-notification (lambda (m p) (declare (ignore m p))))
    (%peer-send peer-out (rpc-protocol:encode-request "ask" nil :id 7))
    (let ((answer (rpc-protocol:decode-message (read-line peer-in))))
      (ok (eql 7 (gethash "id" answer)))
      (ok (eql rpc-protocol:+method-not-found+ (%error-code answer))))))

(deftest serve-refuses-a-client-transport
  (with-raw-peer (client peer-in peer-out :on-notification (lambda (m p) (declare (ignore m p))))
    (ok (signals (rpc-protocol:rpc-serve (lambda (m p) (declare (ignore m)) p) :transport client)
                 'rpc-protocol:rpc-error))))

;;; The server's answers.

(defun %echo-handler (method params)
  (cond ((equal method "echo") params)
        ((equal method "boom") (error "boom ~a" params))
        (t (error 'rpc-protocol:rpc-method-not-found))))

(deftest serve-parse-error-goes-on
  (let ((out (handler-case (%serve-lines #'%echo-handler
                                         "{not json"
                                         (rpc-protocol:encode-request "echo" "after" :id 2))
               (error (e) (format nil "signalled: ~a" e)))))
    (ok (and (listp out) (= 2 (length out))) (format nil "~s" out))
    (ok (eql rpc-protocol:+parse-error+ (%error-code (%nth 0 out))))
    (ok (%null-id-p (%nth 0 out)) "\"id\": null")
    (ok (equal "after" (and (hash-table-p (%nth 1 out)) (gethash "result" (%nth 1 out)))))))

(deftest-parametrize serve-invalid-requests
    ((line id)
     :ids ("number" "string" "old-version" "no-jsonrpc" "no-method" "method-not-a-string")
     ("42" :null)
     ("\"hello\"" :null)
     ("{\"jsonrpc\":\"1.0\",\"method\":\"echo\",\"id\":5}" 5)
     ("{\"method\":\"echo\",\"params\":\"x\",\"id\":9}" 9)
     ("{\"jsonrpc\":\"2.0\",\"id\":6}" 6)
     ("{\"jsonrpc\":\"2.0\",\"method\":7,\"id\":8}" 8))
  (let ((out (handler-case (%serve-lines #'%echo-handler line
                                         (rpc-protocol:encode-request "echo" "next" :id 100))
               (error (e) (format nil "signalled: ~a" e)))))
    (ok (and (listp out) (= 2 (length out))) (format nil "~s" out))
    (ok (eql rpc-protocol:+invalid-request+ (%error-code (%nth 0 out))))
    (ok (if (eq id :null)
            (%null-id-p (%nth 0 out))
            (equal id (and (hash-table-p (%nth 0 out)) (gethash "id" (%nth 0 out)))))
        "the id")
    (ok (equal "next" (and (hash-table-p (%nth 1 out)) (gethash "result" (%nth 1 out))))
        "serving goes on")))

(deftest serve-batch
  (let ((out (handler-case
                 (%serve-lines #'%echo-handler
                               (format nil "[~a,~a,~a]"
                                       (rpc-protocol:encode-request "echo" "a" :id 1)
                                       (rpc-protocol:encode-notification "echo" "n")
                                       (rpc-protocol:encode-request "nope" nil :id 2)))
               (error (e) (format nil "signalled: ~a" e)))))
    (ok (and (listp out) (= 1 (length out))) "one line")
    (let ((batch (%nth 0 out)))
      (ok (vectorp batch) "a JSON array")
      (when (vectorp batch)
        (ok (= 2 (length batch)) "no answer for the notification")
        (ok (equal "a" (gethash "result" (aref batch 0))))
        (ok (eql 1 (gethash "id" (aref batch 0))))
        (ok (eql rpc-protocol:+method-not-found+ (%error-code (aref batch 1))))
        (ok (eql 2 (gethash "id" (aref batch 1))))))))

(deftest serve-batch-edges
  (let ((empty (handler-case (%serve-lines #'%echo-handler "[]") (error (e) (format nil "signalled: ~a" e))))
        (quiet (handler-case (%serve-lines #'%echo-handler
                                           (format nil "[~a]" (rpc-protocol:encode-notification "echo" "n")))
                 (error (e) (format nil "signalled: ~a" e)))))
    (ok (and (listp empty) (= 1 (length empty))))
    (ok (hash-table-p (%nth 0 empty)) "an empty batch gets one error object")
    (ok (eql rpc-protocol:+invalid-request+ (%error-code (%nth 0 empty))))
    (ok (null quiet) "a batch of notifications writes nothing")))

(deftest serve-null-id-and-handler-errors
  (let ((out (%serve-lines #'%echo-handler
                           "{\"jsonrpc\":\"2.0\",\"method\":\"echo\",\"params\":\"x\",\"id\":null}"
                           (rpc-protocol:encode-request "boom" "zap" :id 3))))
    (ok (and (listp out) (= 2 (length out))))
    (ok (%null-id-p (%nth 0 out)))
    (ok (equal "x" (gethash "result" (%nth 0 out))))
    (ok (eql rpc-protocol:+internal-error+ (%error-code (%nth 1 out))))
    (ok (search "boom zap" (gethash "message" (gethash "error" (%nth 1 out)))))))

(deftest serve-ignores-responses-and-blank-lines
  (let ((out (handler-case (%serve-lines #'%echo-handler
                                         ""
                                         "{\"jsonrpc\":\"2.0\",\"result\":1,\"id\":1}"
                                         (rpc-protocol:encode-request "echo" "only" :id 4))
               (error (e) (format nil "signalled: ~a" e)))))
    (ok (and (listp out) (= 1 (length out))) (format nil "~s" out))
    (ok (equal "only" (and (hash-table-p (%nth 0 out)) (gethash "result" (%nth 0 out)))))))

;;; A child process.

#+sbcl
(defun %child-argv ()
  (let ((dirs '()))
    (dolist (name (asdf:already-loaded-systems))
      (let ((dir (ignore-errors (asdf:system-source-directory name))))
        (when dir (pushnew (uiop:native-namestring dir) dirs :test #'string=))))
    (append
     (list (uiop:native-namestring sb-ext:*runtime-pathname*)
           "--core" (uiop:native-namestring sb-ext:*core-pathname*) "--noinform"
           "--non-interactive" "--no-sysinit" "--no-userinit")
     (loop for form in
           (list "(require :asdf)"
                 (format nil "(asdf:initialize-source-registry '~s)"
                         `(:source-registry ,@(mapcar (lambda (d) (list :directory d)) dirs)
                                            :ignore-inherited-configuration))
                 "(let ((*standard-output* (make-broadcast-stream))) (asdf:load-system \"rpc-backend-stdio\"))"
                 "(progn (format *error-output* \"child stderr line~%\") (finish-output *error-output*))"
                 "(let ((tx (rpc-backend-stdio:make-stdio-rpc-transport))) (rpc-protocol:rpc-serve (lambda (m p) (when (equal m \"chatty\") (rpc-protocol:rpc-notify \"tick\" 1 :transport tx) (rpc-protocol:rpc-notify \"tick\" 2 :transport tx)) p) :transport tx))")
           append (list "--eval" form)))))

(deftest command-child-round-trip
  #-sbcl (skip "the child is started with SBCL")
  #+sbcl
  (let* ((log (merge-pathnames (format nil "rpc-stdio-child-~36r.log" (random (expt 36 8) (make-random-state t)))
                               (uiop:temporary-directory)))
         (seen '())
         (lock (bt2:make-lock))
         (tx (rpc-backend-stdio:make-stdio-rpc-transport
              :command (%child-argv)
              :error-output log
              :on-notification (lambda (m p) (bt2:with-lock-held (lock) (push (list m p) seen))))))
    (unwind-protect
         (progn
           (let ((got (handler-case (rpc-protocol:rpc-call "chatty" "hi" :transport tx :timeout 120)
                        (error (e) (list :error (princ-to-string e))))))
             (ok (equal "hi" got) (format nil "the child may compile first; got ~s" got)))
           (ok (%wait-until (lambda () (= 2 (length seen)))))
           (ok (equal '(("tick" 1) ("tick" 2)) (reverse seen)))
           (let ((proc (rpc-backend-stdio:stdio-rpc-transport-process tx)))
             (ok proc)
             (rpc-backend-stdio:close-stdio-rpc-transport tx :timeout 30)
             (ok (eql 0 (process-protocol:exit-code proc))
                 "the child saw EOF on stdin and exited by itself"))
           (ok (search "child stderr line" (or (uiop:read-file-string log) ""))
               ":error-output reached the child"))
      (rpc-backend-stdio:close-stdio-rpc-transport tx :force t)
      (uiop:delete-file-if-exists log))))
