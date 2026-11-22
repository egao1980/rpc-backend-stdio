(in-package #:rpc-backend-stdio/tests)

;;; 0.3.1: a peer that serves (ON-REQUEST) answers parse and envelope errors
;;; as the server loop does (#14); a last line cut short by EOF is never
;;; answered; CLOSE-STDIO-RPC-TRANSPORT's wait for EOF on a child's stdout is
;;; bounded by :GRACE when another process holds the pipe (#12). Helpers are
;;; in peer-test.lisp and dispatch-test.lisp.

(defun %peer-handler (method params)
  (if (equal method "echo")
      params
      (error 'rpc-protocol:rpc-method-not-found)))

(defun %first-answer (message)
  "MESSAGE, or its first element when it is a batch (a JSON array)."
  (if (and (vectorp message) (not (stringp message)) (plusp (length message)))
      (aref message 0)
      message))

(defun %ask-raw (peer-in peer-out line)
  "Send LINE, read one answer line. → (values the decoded answer, the raw line)."
  (%peer-send peer-out line)
  (let ((answer (read-line peer-in)))
    (values (rpc-protocol:decode-message answer) answer)))

(deftest-parametrize peer-answers-bad-lines
    ((dispatch) :ids ("reader" "serial" "concurrent") (:reader) (:serial) (:concurrent))
  (with-raw-peer (client peer-in peer-out :dispatch dispatch :on-request #'%peer-handler)
    (multiple-value-bind (r wire) (%ask-raw peer-in peer-out "{not json")
      (ok (eql rpc-protocol:+parse-error+ (%error-code r)) "not JSON → -32700")
      (ok (%null-id-p r) "\"id\": null")
      (ok (%wire-null-id-p wire) (format nil "wire: ~a" wire)))
    (ok (equal "after" (gethash "result" (%ask-raw peer-in peer-out
                                                   (rpc-protocol:encode-request "echo" "after" :id 1))))
        "the connection keeps working after a parse error")
    (loop for (line id) in '(("42" :null)
                             ("\"hello\"" :null)
                             ("[1]" :null)
                             ("[]" :null)
                             ("{\"method\":\"echo\",\"id\":9}" 9)
                             ("{\"jsonrpc\":\"2.0\",\"id\":6}" 6)
                             ("{\"jsonrpc\":\"2.0\",\"method\":7,\"id\":\"s8\"}" "s8"))
          do (multiple-value-bind (answer wire) (%ask-raw peer-in peer-out line)
               (let ((r (%first-answer answer)))
                 (ok (eql rpc-protocol:+invalid-request+ (%error-code r)) (format nil "~a → -32600" line))
                 (ok (if (eq id :null)
                         (and (%null-id-p r) (%wire-null-id-p wire))
                         (equal id (gethash "id" r)))
                     (format nil "~a: the id" line)))))
    (ok (equal "still" (gethash "result" (%ask-raw peer-in peer-out
                                                   (rpc-protocol:encode-request "echo" "still" :id 2))))
        "and after invalid requests")))

(deftest peer-without-on-request-drops-bad-lines
  (with-raw-peer (client peer-in peer-out :on-notification (lambda (m p) (declare (ignore m p))))
    (%peer-send peer-out "{not json")
    (%peer-send peer-out "42")
    (let ((r (%ask-raw peer-in peer-out (rpc-protocol:encode-request "ask" nil :id 7))))
      (ok (eql 7 (gethash "id" r)) "the first answer is the request's: junk was dropped")
      (ok (eql rpc-protocol:+method-not-found+ (%error-code r))))))

(deftest peer-does-not-answer-error-responses
  (with-raw-peer (client peer-in peer-out :on-request #'%peer-handler)
    ;; The other peer's -32700: a response, so no answer (no error ping-pong).
    (%peer-send peer-out "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32700,\"message\":\"Parse error\"},\"id\":null}")
    (let ((r (%ask-raw peer-in peer-out (rpc-protocol:encode-request "echo" "x" :id 3))))
      (ok (eql 3 (gethash "id" r)) "the error response was dropped")
      (ok (equal "x" (gethash "result" r))))))

(deftest-parametrize peer-partial-line-at-eof
    ((dispatch) :ids ("reader" "concurrent") (:reader) (:concurrent))
  (with-raw-peer (client peer-in peer-out :dispatch dispatch :on-request #'%peer-handler)
    (write-string "{\"jsonrpc\":\"2.0\",\"meth" peer-out)
    (finish-output peer-out)
    (close peer-out)
    (ok (rpc-backend-stdio:stdio-rpc-transport-wait-closed client :timeout 5)
        "EOF is seen: no hang")
    (rpc-backend-stdio:close-stdio-rpc-transport client)
    (ok (eq :eof (read-line peer-in nil :eof)) "nothing was answered")))

(deftest server-answers-bad-lines-over-pipes
  (multiple-value-bind (server-in peer-out) (rpc-backend-stdio:make-stdio-pipe)
    (multiple-value-bind (peer-in server-out) (rpc-backend-stdio:make-stdio-pipe)
      (let* ((server (rpc-backend-stdio:make-stdio-rpc-transport :input server-in :output server-out))
             (thread (bt2:make-thread (lambda ()
                                        (ignore-errors (rpc-protocol:rpc-serve #'%peer-handler :transport server)))
                                      :name "errors-serve")))
        (unwind-protect
             (progn
               (multiple-value-bind (r wire) (%ask-raw peer-in peer-out "{not json")
                 (ok (eql rpc-protocol:+parse-error+ (%error-code r)))
                 (ok (%null-id-p r))
                 (ok (%wire-null-id-p wire) (format nil "wire: ~a" wire)))
               (let ((r (%ask-raw peer-in peer-out "{\"method\":\"echo\",\"id\":4}")))
                 (ok (eql rpc-protocol:+invalid-request+ (%error-code r)))
                 (ok (eql 4 (gethash "id" r))))
               (ok (equal "on" (gethash "result" (%ask-raw peer-in peer-out
                                                           (rpc-protocol:encode-request "echo" "on" :id 5))))
                   "serving goes on")
               ;; A last line cut short by EOF: not answered, the loop ends.
               (write-string "{\"jsonrpc\"" peer-out)
               (finish-output peer-out)
               (close peer-out)
               (ok (%join thread 5) "RPC-SERVE returns at EOF")
               (close server-out)
               (ok (eq :eof (read-line peer-in nil :eof)) "the partial line got no answer"))
          (ignore-errors (close peer-out))
          (ignore-errors (close server-out)))))))

(deftest server-partial-line-at-eof-string-streams
  (ok (null (%serve-lines #'%peer-handler)) "nothing in, nothing out")
  (let* ((in (make-string-input-stream "{\"jsonrpc\":\"2.0\",\"id\""))
         (out (make-string-output-stream))
         (tx (rpc-backend-stdio:make-stdio-rpc-transport :input in :output out)))
    (rpc-protocol:rpc-serve #'%peer-handler :transport tx)
    (ok (equal "" (get-output-stream-string out)) "a truncated last line is not answered")))

;;; #12: a child that leaves a grandchild holding its stdout.

#+sbcl
(defun %sbcl-argv (&rest forms)
  (append (list (uiop:native-namestring sb-ext:*runtime-pathname*)
                "--core" (uiop:native-namestring sb-ext:*core-pathname*) "--noinform"
                "--non-interactive" "--no-sysinit" "--no-userinit")
          (loop for form in forms append (list "--eval" form))))

#+sbcl
(defun %pipe-holder-argv (seconds)
  "A child that starts a grandchild sharing its stdout (alive SECONDS), says
ready, and exits at EOF on stdin."
  (let ((grandchild (%sbcl-argv (format nil "(sleep ~d)" seconds))))
    (%sbcl-argv
     (format nil "(sb-ext:run-program ~s '~s :input nil :output t :error nil :wait nil)"
             (first grandchild) (rest grandchild))
     "(progn (write-line \"{\\\"jsonrpc\\\":\\\"2.0\\\",\\\"method\\\":\\\"ready\\\"}\") (finish-output))"
     "(loop while (read-line *standard-input* nil))"
     "(sb-ext:exit :code 0 :abort t)")))

(deftest close-bounded-when-a-grandchild-holds-stdout
  #-sbcl (skip "the child is started with SBCL")
  #+sbcl
  (let* ((ready nil)
         (warned nil)
         (tx (rpc-backend-stdio:make-stdio-rpc-transport
              :command (%pipe-holder-argv 40)
              :on-notification (lambda (m p) (declare (ignore p))
                                 (when (equal m "ready") (setf ready t))))))
    (unwind-protect
         (progn
           (ok (rpc-protocol:rpc-notify "hello" nil :transport tx) "the child is launched")
           (ok (%wait-until (lambda () ready) :timeout 120) "the grandchild runs")
           (let* ((proc (rpc-backend-stdio:stdio-rpc-transport-process tx))
                  (start (get-internal-real-time)))
             (handler-bind ((rpc-backend-stdio:stdio-rpc-pipe-held
                              (lambda (w) (setf warned w) (muffle-warning w))))
               (rpc-backend-stdio:close-stdio-rpc-transport tx :timeout 20 :grace 1))
             (let ((elapsed (%elapsed-since start)))
               (ok (< elapsed 15) (format nil "close returned after ~,1f s (grandchild lives 40 s)" elapsed)))
             (ok (typep warned 'rpc-backend-stdio:stdio-rpc-pipe-held) "the held pipe was reported")
             (ok (and warned (eql 1 (rpc-backend-stdio:stdio-rpc-pipe-held-seconds warned))))
             (ok (eql 0 (process-protocol:exit-code proc)) "the child exited at EOF by itself")
             (ok (signals (rpc-protocol:rpc-call "echo" 1 :transport tx :timeout 1) 'rpc-protocol:rpc-error)
                 "the transport is closed")))
      (handler-bind ((warning #'muffle-warning))
        (rpc-backend-stdio:close-stdio-rpc-transport tx :force t)))))
