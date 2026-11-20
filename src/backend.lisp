(in-package #:rpc-backend-stdio)

;;; Newline-delimited JSON-RPC 2.0 over a pair of character streams: the
;;; process's own stdin / stdout, a child's pipes (:COMMAND), or the
;;; in-memory pipes of MAKE-STDIO-RPC-PAIR (src/pipe.lisp). One JSON value per
;;; line, UTF-8, LF; every line is written whole under the transport's write
;;; lock (0.1.2), so notifications sent from other threads never interleave
;;; with a response.
;;;
;;; 0.2.0 (contract). A transport is used either as a SERVER (RPC-SERVE) or
;;; as a CLIENT (RPC-CALL). RPC-NOTIFY works on both: a server sends its
;;; notifications with it while RPC-SERVE runs.
;;;
;;; Server (BACKEND-RPC-SERVE): read lines until EOF, then return the
;;; transport. Each line is handled before the next is read (one request at
;;; a time; a handler that must not block the reader hands work to another
;;; thread and returns). Every line gets the JSON-RPC 2.0 answer, and the
;;; loop always goes on:
;;;   not JSON                    → error -32700 "Parse error", "id": null
;;;   not an object or an array   → error -32600 "Invalid Request", "id": null
;;;   an object whose "jsonrpc" is not "2.0", or whose "method" is missing or
;;;   not a string                → error -32600 "Invalid Request", with the
;;;                                 object's "id" when it has a number or a
;;;                                 string there, else "id": null
;;;   a request (has "id")        → the HANDLER's result, or its error:
;;;                                 RPC-ERROR → its code, message and data;
;;;                                 any other ERROR → -32603 with the
;;;                                 condition's text; an "id" of null is
;;;                                 answered with "id": null
;;;   a notification (no "id")    → HANDLER called, nothing written (errors
;;;                                 are dropped)
;;;   a response (has "result" or "error", no "method")
;;;                               → ignored (a server sends no requests)
;;;   an array (a batch)          → each element as above, in order; the
;;;                                 answers that exist go out as ONE line, a
;;;                                 JSON array; a batch of notifications only
;;;                                 writes nothing; an empty array → one
;;;                                 -32600 error object (not an array)
;;;   a blank line                → ignored
;;; HANDLER is (lambda (method params)); PARAMS is the decoded "params"
;;; (an object, an array, or NIL when absent). A transport whose reader
;;; thread runs (a client) signals RPC-ERROR -32603 "the transport is a
;;; client" from RPC-SERVE.
;;;
;;; Client. A reader thread owns the input stream. It starts with the first
;;; RPC-CALL, or at once when MAKE-STDIO-RPC-TRANSPORT gets :ON-NOTIFICATION
;;; or :ON-REQUEST (so notifications that arrive before any call are not
;;; lost). It reads one line at a time and routes it:
;;;   a response ("result" or "error", and an "id")
;;;                → the RPC-CALL waiting on that id (ids are matched with
;;;                  EQUAL on the decoded value); a response nobody waits for
;;;                  is dropped
;;;   a notification ("method", no "id")
;;;                → (funcall on-notification method params) on the reader
;;;                  thread, in arrival order; dropped without a handler; an
;;;                  error in the handler is dropped and reading goes on
;;;   a request ("method" and "id"), sent by the server
;;;                → (funcall on-request method params) on the reader thread;
;;;                  its value is the result, an RPC-ERROR its error, any other
;;;                  error -32603; without ON-REQUEST → -32601 "Method not
;;;                  found". The answer is written with the write lock.
;;;   anything else (not JSON, not an object) → dropped
;;; Several threads may call at once; each RPC-CALL writes its request and
;;; waits for its own id. Ids are the transport's counter (1, 2, …) unless
;;; :ID is given. An error response → RPC-ERROR with its code, message and
;;; data (as 0.1). :TIMEOUT (seconds, NIL = wait forever) → STDIO-RPC-TIMEOUT
;;; when no response came in time (a late response is dropped). When the
;;; input reaches EOF or fails, every waiting call and every later call
;;; signals RPC-ERROR -32603 "stdio RPC peer closed".
;;;
;;; :COMMAND launches the child through process-protocol on first use (as
;;; 0.1). :ERROR-OUTPUT goes to PROCESS-PROTOCOL:LAUNCH's :ERROR-OUTPUT;
;;; default :INTERACTIVE (the child's stderr is the parent's), so a chatty
;;; child never blocks on an undrained stderr pipe (0.1 left it a :STREAM
;;; nobody read). STDIO-RPC-TRANSPORT-PROCESS is the launched handle, or NIL.
;;;
;;; CLOSE-STDIO-RPC-TRANSPORT TRANSPORT &key FORCE (TIMEOUT 5): with a child,
;;; close the child's stdin first (a JSON-RPC server exits at EOF), wait up
;;; to TIMEOUT seconds for it to exit, then kill it (FORCE: at once, without
;;; the wait). The reader thread is stopped and joined. Calls still waiting
;;; get the "peer closed" RPC-ERROR. Idempotent. → TRANSPORT.

(defclass stdio-rpc-transport (rpc-protocol:rpc-transport)
  ((input :initarg :input :initform nil :accessor transport-input)
   (output :initarg :output :initform nil :accessor transport-output)
   (command :initarg :command :initform nil :accessor transport-command)
   (process :initform nil :accessor transport-process)
   (next-id :initform 0 :accessor transport-next-id)
   (write-lock :initform (bt2:make-lock :name "stdio-rpc write") :reader transport-write-lock
               :documentation "Held for each line written, so a notification sent from
another thread never interleaves with a response.")
   ;; 0.2.0 (given; the behaviour is the implementation's, see the header)
   (on-notification :initarg :on-notification :initform nil
                    :accessor stdio-rpc-transport-on-notification
                    :documentation "Client: (lambda (method params)) for notifications, on the reader thread.")
   (on-request :initarg :on-request :initform nil
               :accessor stdio-rpc-transport-on-request
               :documentation "Client: (lambda (method params)) → result, for requests the server sends.")
   (error-output :initarg :error-output :initform :interactive :reader transport-error-output
                 :documentation ":COMMAND: the child's stderr (PROCESS-PROTOCOL:LAUNCH :ERROR-OUTPUT).")
   (reader :initform nil :accessor transport-reader
           :documentation "The client's reader thread, or NIL.")
   (pending :initform (make-hash-table :test 'equal) :reader transport-pending
            :documentation "Client: id → the waiting call's cell.")
   (state-lock :initform (bt2:make-lock :name "stdio-rpc state") :reader transport-state-lock)
   (closed :initform nil :accessor transport-closed
           :documentation "True once the input reached EOF or the transport was closed.")
   ;; 0.2.0 (internal)
   (state-cv :initform (bt2:make-condition-variable :name "stdio-rpc state")
             :reader transport-state-cv
             :documentation "Broadcast, under STATE-LOCK, whenever a pending cell is
answered or CLOSED becomes true; RPC-CALL waits on it.")
   (closed-by-user :initform nil :accessor transport-closed-by-user
                   :documentation "True once CLOSE-STDIO-RPC-TRANSPORT ran (idempotence).")))

(defun stdio-rpc-transport-process (transport)
  "The launched child's process handle (:COMMAND), or NIL."
  (transport-process transport))

(define-condition stdio-rpc-timeout (rpc-protocol:rpc-error)
  ((method :initarg :method :initform nil :reader stdio-rpc-timeout-method)
   (seconds :initarg :seconds :initform nil :reader stdio-rpc-timeout-seconds))
  (:default-initargs :code rpc-protocol:+internal-error+)
  (:documentation "RPC-CALL :TIMEOUT passed with no response.")
  (:report (lambda (c s)
             (format s "stdio RPC call ~s timed out after ~a s"
                     (stdio-rpc-timeout-method c) (stdio-rpc-timeout-seconds c)))))

(defun %has-handler-p (transport)
  (or (stdio-rpc-transport-on-notification transport)
      (stdio-rpc-transport-on-request transport)))

(defun make-stdio-rpc-transport (&key input output command (error-output :interactive)
                                      on-notification on-request)
  "A transport on INPUT / OUTPUT (default *STANDARD-INPUT* / *STANDARD-OUTPUT*
when used), or on the pipes of COMMAND launched on first use. ON-NOTIFICATION
or ON-REQUEST make it a client whose reader thread starts at once (see the
header)."
  (let ((transport (make-instance 'stdio-rpc-transport
                                  :input input :output output :command command
                                  :error-output error-output
                                  :on-notification on-notification :on-request on-request)))
    ;; With :COMMAND the child is launched on first use, and the reader
    ;; starts right after the launch (%ENSURE-IO).
    (when (and (%has-handler-p transport) (null command))
      (multiple-value-call #'%ensure-reader transport (%ensure-io transport)))
    transport))

(defun use-stdio-rpc-transport (&rest args &key &allow-other-keys)
  (setf rpc-protocol:*rpc-transport*
        (apply #'make-stdio-rpc-transport args)))

;;; Conditions signalled by the client.

(defun %peer-closed (&optional detail)
  (error 'rpc-protocol:rpc-error
         :message (format nil "stdio RPC peer closed~@[ (~a)~]" detail)
         :code rpc-protocol:+internal-error+))

;;; Streams.

(defun %ensure-io (transport)
  "→ (values input output), the streams the transport uses now. Launches
:COMMAND on first use (and starts the reader when a handler is given).
Runs in the caller's thread: *STANDARD-INPUT* / *STANDARD-OUTPUT* are
resolved here, since a new thread does not see the caller's bindings.
The defaulted standard streams are never stored in the slots, so
CLOSE-STDIO-RPC-TRANSPORT never closes them."
  (when (and (transport-command transport)
             (null (transport-process transport)))
    (when (transport-closed-by-user transport)
      (%peer-closed "the transport was closed"))
    (unless process-protocol:*process-backend*
      (error 'rpc-protocol:rpc-error
             :message "*process-backend* is nil — load process-backend-uiop"
             :code rpc-protocol:+internal-error+))
    (let ((handle (process-protocol:launch (transport-command transport)
                                           :error-output (transport-error-output transport))))
      (setf (transport-process transport) handle
            (transport-input transport) (process-protocol:stdout handle)
            (transport-output transport) (process-protocol:stdin handle))
      (when (%has-handler-p transport)
        (%ensure-reader transport (transport-input transport) (transport-output transport)))))
  (values (or (transport-input transport) *standard-input*)
          (or (transport-output transport) *standard-output*)))

(defun %write-json-line (stream string &optional transport)
  (flet ((emit ()
           (write-string string stream)
           (write-char #\newline stream)
           (finish-output stream)))
    (if transport
        (bt2:with-lock-held ((transport-write-lock transport)) (emit))
        (emit))))

(defun %raise-rpc (msg)
  (let ((err (gethash "error" msg)))
    (if err
        (error 'rpc-protocol:rpc-error
               :code (or (and (hash-table-p err) (gethash "code" err))
                         rpc-protocol:+internal-error+)
               :message (and (hash-table-p err) (gethash "message" err))
               :data (and (hash-table-p err) (gethash "data" err)))
        (gethash "result" msg))))

;;; Messages, shared by the server loop and the client's reader.

(defun %key-p (msg key)
  (nth-value 1 (gethash key msg)))

(defun %classify (msg)
  "→ :INVALID, :RESPONSE, :REQUEST or :NOTIFICATION for a decoded message.
A response is checked before the envelope: it has no \"method\"."
  (cond ((not (hash-table-p msg)) :invalid)
        ((and (or (%key-p msg "result") (%key-p msg "error"))
              (not (%key-p msg "method")))
         :response)
        ((not (and (equal "2.0" (gethash "jsonrpc" msg))
                   (stringp (gethash "method" msg))))
         :invalid)
        ((%key-p msg "id") :request)
        (t :notification)))

(defun %invalid-request (msg)
  "The -32600 wire answer for MSG: its id when that is a number or a string."
  (let ((id (and (hash-table-p msg) (gethash "id" msg))))
    (rpc-protocol:encode-error-response
     rpc-protocol:+invalid-request+ "Invalid Request"
     :id (and (or (realp id) (stringp id)) id))))

(defun %answer (handler msg)
  "Call HANDLER on the request or notification MSG. → the wire answer, or NIL
for a notification. RPC-ERROR → its code, message and data; any other ERROR
→ -32603 with the condition's text (the header's answer table)."
  (multiple-value-bind (id has-id) (gethash "id" msg)
    (flet ((fail (code text &optional data)
             (and has-id
                  (rpc-protocol:encode-error-response code text :id id :data data))))
      (handler-case
          (let ((result (funcall handler (gethash "method" msg) (gethash "params" msg))))
            (and has-id (rpc-protocol:encode-response result :id id)))
        (rpc-protocol:rpc-error (c)
          (fail (rpc-protocol:rpc-error-code c)
                (or (rpc-protocol:rpc-error-message c) "rpc error")
                (rpc-protocol:rpc-error-data c)))
        (error (c)
          (fail rpc-protocol:+internal-error+ (princ-to-string c)))))))

(defun %serve-one (handler msg)
  "One object (or batch element) on the server. → a wire string or NIL."
  (ecase (%classify msg)
    (:invalid (%invalid-request msg))
    (:response nil)
    ((:request :notification) (%answer handler msg))))

(defun %blank-line-p (line)
  (every (lambda (c) (member c '(#\Space #\Tab #\Return #\Page))) line))

(defun %serve-line (handler line)
  "The server's answer to one input LINE: a wire string, or NIL."
  (when (%blank-line-p line)
    (return-from %serve-line nil))
  (let ((msg (handler-case (rpc-protocol:decode-message line)
               (error ()
                 (return-from %serve-line
                   (rpc-protocol:encode-error-response
                    rpc-protocol:+parse-error+ "Parse error" :id nil))))))
    (cond ((hash-table-p msg) (%serve-one handler msg))
          ((and (vectorp msg) (not (stringp msg)))
           (if (zerop (length msg))
               (%invalid-request nil)
               (let ((answers (remove nil (map 'list (lambda (e) (%serve-one handler e)) msg))))
                 (and answers (format nil "[~{~a~^,~}]" answers)))))
          ;; numbers, strings, true / false / null (NIL may also be an empty array)
          (t (%invalid-request nil)))))

;;; Client: pending calls and the reader thread.

(defstruct (%cell (:constructor %make-cell ()))
  "One waiting RPC-CALL. STATUS is :WAITING until the reader stores the
response in MESSAGE (:DONE). Read and written under STATE-LOCK."
  (status :waiting)
  (message nil))

(defun %mark-closed (transport)
  "INPUT reached EOF, failed, or the transport was closed: every waiting call
and every later call signals \"peer closed\"."
  (bt2:with-lock-held ((transport-state-lock transport))
    (setf (transport-closed transport) t)
    (bt2:condition-broadcast (transport-state-cv transport))))

(defun %route-response (transport msg)
  (multiple-value-bind (id has-id) (gethash "id" msg)
    (when has-id
      (bt2:with-lock-held ((transport-state-lock transport))
        (let ((cell (gethash id (transport-pending transport))))
          ;; A response nobody waits for (unknown, or timed out) is dropped.
          (when cell
            (remhash id (transport-pending transport))
            (setf (%cell-message cell) msg
                  (%cell-status cell) :done)
            (bt2:condition-broadcast (transport-state-cv transport))))))))

(defun %no-request-handler (method params)
  (declare (ignore method params))
  (error 'rpc-protocol:rpc-method-not-found))

(defun %route-line (transport output line)
  "Route one line read by the client's reader (see the header)."
  (let ((msg (handler-case (rpc-protocol:decode-message line)
               (error () (return-from %route-line nil)))))  ; junk is dropped
    (case (%classify msg)
      (:response (%route-response transport msg))
      (:notification
       (let ((handler (stdio-rpc-transport-on-notification transport)))
         (when handler
           ;; The header: an error in the handler is dropped, reading goes on.
           (handler-case (funcall handler (gethash "method" msg) (gethash "params" msg))
             (error () nil)))))
      (:request
       (let ((wire (%answer (or (stdio-rpc-transport-on-request transport)
                                #'%no-request-handler)
                            msg)))
         (when wire
           ;; A server that went away cannot be answered; the answer is dropped.
           (handler-case (%write-json-line output wire transport)
             (stream-error () nil))))))))

(defun %reader-loop (transport input output)
  (catch '%stop-reader                  ; CLOSE-STDIO-RPC-TRANSPORT's last resort
    (unwind-protect
         ;; A read failure (a stream closed under the reader, a decoding
         ;; error) is the peer closing: nothing escapes the thread function,
         ;; so the reader never reaches the debugger.
         (handler-case
             (loop for line = (read-line input nil :eof)
                   until (eq line :eof)
                   do (handler-case (%route-line transport output line)
                        (error () nil)))
           (error () nil))
      (%mark-closed transport))))

(defun %ensure-reader (transport input output)
  "Start the reader thread on INPUT unless it runs or the transport is closed."
  (bt2:with-lock-held ((transport-state-lock transport))
    (unless (or (transport-reader transport) (transport-closed transport))
      (setf (transport-reader transport)
            (bt2:make-thread (lambda () (%reader-loop transport input output))
                             :name "stdio-rpc reader"))))
  transport)

(defun %await (transport cell timeout)
  "Wait, under STATE-LOCK, until CELL is answered, the transport closes, or
TIMEOUT seconds pass. → :DONE, :CLOSED or :TIMEOUT."
  (let ((lock (transport-state-lock transport))
        (cv (transport-state-cv transport))
        (deadline (and timeout
                       (+ (get-internal-real-time)
                          (round (* timeout internal-time-units-per-second))))))
    (bt2:with-lock-held (lock)
      (loop
        (cond ((eq :done (%cell-status cell)) (return :done))
              ((transport-closed transport) (return :closed)))
        (if deadline
            (let ((remaining (/ (- deadline (get-internal-real-time))
                                internal-time-units-per-second)))
              (when (<= remaining 0) (return :timeout))
              (bt2:condition-wait cv lock :timeout remaining))
            (bt2:condition-wait cv lock))))))

(defmethod rpc-protocol:backend-rpc-call
    ((transport stdio-rpc-transport) method params &key timeout id)
  (multiple-value-bind (in out) (%ensure-io transport)
    (let ((cell (%make-cell))
          (pending (transport-pending transport)))
      ;; Register before writing: a fast peer must find the cell.
      (bt2:with-lock-held ((transport-state-lock transport))
        (when (transport-closed transport)
          (%peer-closed))
        (setf id (or id (incf (transport-next-id transport)))
              (gethash id pending) cell))
      (unwind-protect
           (progn
             (%ensure-reader transport in out)
             (handler-case
                 (%write-json-line out (rpc-protocol:encode-request method params :id id) transport)
               (stream-error (e) (%peer-closed (princ-to-string e))))
             (ecase (%await transport cell timeout)
               (:done (%raise-rpc (%cell-message cell)))
               (:closed (%peer-closed))
               (:timeout
                (error 'stdio-rpc-timeout
                       :method method :seconds timeout
                       :message (format nil "~s timed out after ~a s" method timeout)))))
        ;; Timeout, peer closed or unwind: a late response finds no cell.
        (bt2:with-lock-held ((transport-state-lock transport))
          (when (eq cell (gethash id pending))
            (remhash id pending)))))))

(defmethod rpc-protocol:backend-rpc-notify
    ((transport stdio-rpc-transport) method params)
  (multiple-value-bind (in out) (%ensure-io transport)
    (declare (ignore in))
    (%write-json-line out (rpc-protocol:encode-notification method params) transport)
    t))

(defmethod rpc-protocol:backend-rpc-serve
    ((transport stdio-rpc-transport) handler &key)
  (when (transport-reader transport)
    (error 'rpc-protocol:rpc-error
           :message "the transport is a client (its reader thread runs)"
           :code rpc-protocol:+internal-error+))
  (multiple-value-bind (in out) (%ensure-io transport)
    (loop for line = (read-line in nil :eof)
          until (eq line :eof)
          do (let ((wire (%serve-line handler line)))
               (when wire
                 (%write-json-line out wire transport)))))
  transport)

;;; Close.

(defgeneric %close-wakes-reader-p (stream)
  (:documentation "True when closing STREAM from another thread is safe and
ends a read blocked on it (the in-memory pipes of src/pipe.lisp).")
  (:method ((stream t)) nil))

(defun %close-quietly (stream)
  ;; Close's own cleanup: a stream that fails to close is already gone.
  (when stream
    (handler-case (close stream) (error () nil))))

(defun %close-output (transport)
  "Close OUTPUT (the peer sees EOF). The write lock is taken when it can be,
so a line is not cut in half; a writer blocked on a full pipe holds it, and
closing the pipe is what wakes that writer, so the close does not wait for
it more than a second."
  (let ((out (transport-output transport))
        (lock (transport-write-lock transport)))
    (when out
      (if (bt2:acquire-lock lock :timeout 1)
          (unwind-protect (%close-quietly out)
            (bt2:release-lock lock))
          (%close-quietly out)))))

(defun %stop-process (process force timeout)
  (unless force
    (handler-case (process-protocol:wait process :timeout timeout)
      (process-protocol:process-timeout-error () nil)))
  (when (process-protocol:alive-p process)
    (handler-case (process-protocol:kill process :force t)
      (error () nil))
    (handler-case (process-protocol:wait process :timeout 5)
      (process-protocol:process-timeout-error () nil))))

(defun %join-reader (thread seconds)
  "Join THREAD when it ends within SECONDS. → T, or NIL while it still runs."
  (let ((deadline (+ (get-internal-real-time)
                     (round (* seconds internal-time-units-per-second)))))
    (loop while (and (bt2:thread-alive-p thread)
                     (< (get-internal-real-time) deadline))
          do (sleep 0.01))
    (unless (bt2:thread-alive-p thread)
      (handler-case (bt2:join-thread thread) (error () nil))
      t)))

(defun close-stdio-rpc-transport (transport &key force (timeout 5))
  "See the header (0.2.0: stdin first, TIMEOUT, the reader thread)."
  (when (bt2:with-lock-held ((transport-state-lock transport))
          (shiftf (transport-closed-by-user transport) t))
    (return-from close-stdio-rpc-transport transport))
  (let ((process (transport-process transport))
        (input (transport-input transport))
        (reader (transport-reader transport)))
    ;; 1. Our output: the child's stdin, or the pipe the peer reads.
    (%close-output transport)
    ;; 2. The child: exits at EOF, or is killed.
    (when process
      (%stop-process process force timeout))
    ;; 3. The reader. An in-memory pipe wakes its reader on close; a child's
    ;;    stdout reaches EOF when it exits. Other streams are closed after a
    ;;    short wait, which ends a blocked read with an error (caught there).
    (when (%close-wakes-reader-p input)
      (%close-quietly input))
    (when reader
      (unless (%join-reader reader (if process 5 0.5))
        (%close-quietly input)
        (unless (%join-reader reader 2)
          ;; Last resort: a read that close cannot wake (a foreign stream).
          ;; The reader unwinds to its CATCH, marking the transport closed.
          (handler-case
              (bt2:interrupt-thread reader (lambda () (throw '%stop-reader nil)))
            (error () nil))
          (%join-reader reader 1))))
    (%close-quietly input)
    ;; 4. Waiting calls get "peer closed".
    (%mark-closed transport))
  transport)

(use-stdio-rpc-transport)
