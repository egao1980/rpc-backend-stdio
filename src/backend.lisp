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
;;;   anything else (not JSON, not an object) → dropped (0.3.1: answered
;;;                when the transport has ON-REQUEST, see below)
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
;;;
;;; 0.3.0: request dispatch (client mode). MAKE-STDIO-RPC-TRANSPORT
;;; :DISPATCH picks the thread that runs ON-REQUEST and ON-NOTIFICATION:
;;;   :READER (default, 0.2.0) → on the reader thread, as above. A handler
;;;                that calls RPC-CALL on the same transport would wait for
;;;                a response only the reader can read, so that call signals
;;;                RPC-ERROR -32603 at once instead of deadlocking.
;;;   :SERIAL     → one dispatcher thread runs every incoming request and
;;;                notification, one at a time, in arrival order. The reader
;;;                only routes, so a handler may RPC-CALL the peer and wait
;;;                (ACP: session/prompt → session/request_permission). A
;;;                request that arrives while a handler waits is queued
;;;                behind it.
;;;   :CONCURRENT → each request runs on a thread of its own, at once;
;;;                notifications run on one dispatcher thread, in arrival
;;;                order (session/update stays ordered, and a session/cancel
;;;                notification is seen while session/prompt still runs).
;;; Guarantees in every mode: notifications are handled in arrival order,
;;; one at a time; responses are routed to their calls by the reader as
;;; they arrive, whatever the handlers do; each answer is written whole
;;; under the write lock when its handler returns (under :CONCURRENT answers
;;; may go out in any order — JSON-RPC matches them by id). The 0.2.0 answer
;;; table holds: a handler's RPC-ERROR → its code, message and data, any
;;; other ERROR → -32603, no ON-REQUEST → -32601; notification handler
;;; errors are dropped.
;;; *STDIO-RPC-REQUEST-ID* is bound to the request's id while ON-REQUEST (or
;;; a server HANDLER) runs, NIL for a notification, so a layer above can map
;;; a cancel notification to the running handler.
;;; Shutdown: CLOSE-STDIO-RPC-TRANSPORT drops queued messages that have not
;;; started, makes every RPC-CALL a handler waits in signal "peer closed",
;;; and joins the dispatcher and the workers; one still running after 2 s is
;;; interrupted and unwound. At the peer's EOF the messages already read are
;;; still handled (their answers are dropped when the peer cannot read).
;;; STDIO-RPC-TRANSPORT-WAIT-CLOSED TRANSPORT &key TIMEOUT: start the reader
;;; if needed and block until the input reached EOF (or the transport was
;;; closed) and no handler runs or waits in the queue. → T, or NIL when
;;; TIMEOUT seconds passed first. An agent process's main thread waits here.
;;;
;;; 0.3.1: peer-mode errors (#14). A transport with ON-REQUEST serves, so its
;;; reader answers bad lines as the server loop does, at once, on the reader
;;; thread, under the write lock (whatever :DISPATCH says), and reads on:
;;;   not JSON                    → -32700 "Parse error", "id": null
;;;   not an object or an array   → -32600 "Invalid Request", "id": null
;;;   an invalid envelope         → -32600 with its id (number or string) or null
;;;   an array                    → each element routed as a line of its own;
;;;                                 the -32600 answers to its invalid elements
;;;                                 go out as ONE array line; an empty array →
;;;                                 one -32600 object. Requests in it are
;;;                                 answered one by one, not as a batch.
;;; Error responses carry no "method", so they are :RESPONSEs: two peers never
;;; answer each other's errors. Without ON-REQUEST bad lines are still
;;; dropped. Blank lines are ignored. A last line cut short by EOF (no LF) is
;;; never answered with -32700, by the reader or the server loop.
;;;
;;; 0.3.1: bounded close (#12). CLOSE-STDIO-RPC-TRANSPORT … &key (GRACE 3):
;;; once the child exited (or was killed), the reader gets GRACE seconds to
;;; reach EOF on the child's stdout. Another process may hold that pipe (a
;;; grandchild that inherited it); then STDIO-RPC-PIPE-HELD (a WARNING) is
;;; signalled, the stream is closed on a helper thread (closing a Windows
;;; pipe under a pending read blocks; that close is waited for 1 s at most),
;;; the reader is interrupted, and close returns. A reader that still cannot
;;; be woken is left behind; it ends when the pipe's last writer goes away.
;;;
;;; 0.3.2: every "id": null above is written as JSON null (:NULL), not false
;;; (#17). "peer closed" is STDIO-RPC-PEER-CLOSED, an RPC-ERROR -32603 with the
;;; same message, so RPC-ERROR handlers still match (#16). MAKE-STDIO-RPC-TRANSPORT
;;; and MAKE-STDIO-RPC-PAIR take :CLASS, a subclass of STDIO-RPC-TRANSPORT to
;;; instantiate (#16).
;;;
;;; 0.4.0: the :COMMAND child's environment and directory. MAKE-STDIO-RPC-TRANSPORT
;;; takes :ENV, :ENV-MODE (default :MERGE) and :DIRECTORY, passed as they are to
;;; PROCESS-PROTOCOL:LAUNCH (ENV entries are (name . value) or "NAME=VALUE"; with
;;; :MERGE they go over the inherited environment and a NIL value unsets; with
;;; :REPLACE the child gets exactly ENV). NIL (the default) leaves the child the
;;; parent's environment and working directory, as before. They mean nothing
;;; without :COMMAND (egao1980/mcp-backend-stdio#7).
;;;
;;; 0.4.1: barrier requests (egao1980/cl-stack-code#45). MAKE-STDIO-RPC-TRANSPORT
;;; takes :BARRIER-METHODS, a list of method names (strings; default NIL). Under
;;; :CONCURRENT an incoming request whose "method" is one of them is a barrier:
;;; the requests and notifications read after it are held, not dispatched,
;;; until its handler returned and its answer was written; then they are
;;; dispatched as usual, in arrival order (a held barrier holds the ones after
;;; it again). Responses are still routed as they arrive, so the barrier's
;;; handler may call the peer and wait. A peer that pipelines a handshake
;;; request ("initialize") with the requests that need it gets them handled
;;; after it, and its answer first. :SERIAL and :READER handle one message at
;;; a time in arrival order already: there the option changes nothing. Held
;;; messages count as queued: WAIT-CLOSED waits for them, CLOSE-STDIO-RPC-
;;; TRANSPORT drops them. STDIO-RPC-TRANSPORT-BARRIER-METHODS reads the list.

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
   ;; 0.4.0
   (env :initarg :env :initform nil :reader transport-env
        :documentation ":COMMAND: PROCESS-PROTOCOL:LAUNCH :ENV, or NIL.")
   (env-mode :initarg :env-mode :initform :merge :reader transport-env-mode
             :documentation ":COMMAND: PROCESS-PROTOCOL:LAUNCH :ENV-MODE (:MERGE or :REPLACE).")
   (directory :initarg :directory :initform nil :reader transport-directory
              :documentation ":COMMAND: the child's working directory, or NIL (the parent's).")
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
                   :documentation "True once CLOSE-STDIO-RPC-TRANSPORT ran (idempotence).")
   ;; 0.3.0 (dispatch; QUEUE, DISPATCHER, WORKERS and ACTIVE under STATE-LOCK)
   (dispatch :initarg :dispatch :initform :reader :reader stdio-rpc-transport-dispatch
             :documentation ":READER, :SERIAL or :CONCURRENT (see the header).")
   (queue :initform nil :accessor transport-queue
          :documentation "Incoming (kind . message) pairs the dispatcher has not taken, oldest first.")
   (queue-tail :initform nil :accessor transport-queue-tail)
   (dispatcher :initform nil :accessor transport-dispatcher
               :documentation "The dispatcher thread (:SERIAL, :CONCURRENT), or NIL.")
   (workers :initform '() :accessor transport-workers
            :documentation ":CONCURRENT: the request threads still running.")
   (active :initform 0 :accessor transport-active
           :documentation "Handlers running off the reader thread now.")
   ;; 0.4.1 (BARRIER-ACTIVE and HELD under STATE-LOCK)
   (barrier-methods :initarg :barrier-methods :initform nil
                    :reader stdio-rpc-transport-barrier-methods
                    :documentation "Method names whose requests are barriers (:CONCURRENT, see the header).")
   (barrier-active :initform nil :accessor transport-barrier-active
                   :documentation "True from a barrier request's dispatch until its answer was written.")
   (held :initform nil :accessor transport-held
         :documentation "(kind . message) pairs read while a barrier ran, oldest first.")))

(defvar *stdio-rpc-request-id* nil
  "The id of the request whose handler runs in this thread, NIL otherwise.")

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

(define-condition stdio-rpc-peer-closed (rpc-protocol:rpc-error) ()
  (:default-initargs :code rpc-protocol:+internal-error+)
  (:documentation "0.3.2 (#16): the peer closed (EOF or a failed read / write on the
streams) or the transport was closed, so an RPC-CALL cannot be answered. An
RPC-ERROR with code -32603 and the message \"stdio RPC peer closed …\", as
before, so handlers for RPC-ERROR still match."))

(define-condition stdio-rpc-pipe-held (warning)
  ((process :initarg :process :initform nil :reader stdio-rpc-pipe-held-process)
   (seconds :initarg :seconds :initform nil :reader stdio-rpc-pipe-held-seconds))
  (:documentation "CLOSE-STDIO-RPC-TRANSPORT (0.3.1): the child's stdout did not
reach EOF within GRACE seconds of the child's exit, so another process still
holds the pipe. The stream was closed without waiting for EOF.")
  (:report (lambda (c s)
             (format s "stdio RPC: the child's stdout was still held open ~a s after the child exited (another process holds the pipe); closed without waiting for EOF"
                     (stdio-rpc-pipe-held-seconds c)))))

(defun %has-handler-p (transport)
  (or (stdio-rpc-transport-on-notification transport)
      (stdio-rpc-transport-on-request transport)))

(defun %transport-class-p (class)
  (and (or (symbolp class) (typep class 'class))
       (subtypep class 'stdio-rpc-transport)))

(defun make-stdio-rpc-transport (&key input output command (error-output :interactive)
                                      env (env-mode :merge) directory
                                      on-notification on-request (dispatch :reader)
                                      barrier-methods (class 'stdio-rpc-transport))
  "A transport on INPUT / OUTPUT (default *STANDARD-INPUT* / *STANDARD-OUTPUT*
when used), or on the pipes of COMMAND launched on first use. ON-NOTIFICATION
or ON-REQUEST make it a client whose reader thread starts at once. DISPATCH
(0.3.0): :READER, :SERIAL or :CONCURRENT, the thread the handlers run on
(see the header). CLASS (0.3.2, #16): the class to instantiate, STDIO-RPC-TRANSPORT
or a subclass of it. ENV, ENV-MODE and DIRECTORY (0.4.0): the :COMMAND child's
environment and working directory, as PROCESS-PROTOCOL:LAUNCH takes them.
BARRIER-METHODS (0.4.1): method names whose requests are barriers under
:CONCURRENT (see the header)."
  (check-type dispatch (member :reader :serial :concurrent))
  (check-type env-mode (member :merge :replace))
  (check-type class (satisfies %transport-class-p) "STDIO-RPC-TRANSPORT or a subclass (a name or a class)")
  (check-type barrier-methods list)
  (assert (every #'stringp barrier-methods) (barrier-methods)
          "BARRIER-METHODS must be a list of method name strings, not ~s" barrier-methods)
  (let ((transport (make-instance class
                                  :input input :output output :command command
                                  :error-output error-output :dispatch dispatch
                                  :env env :env-mode env-mode :directory directory
                                  :barrier-methods (copy-list barrier-methods)
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
  (error 'stdio-rpc-peer-closed
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
    (let ((handle (apply #'process-protocol:launch (transport-command transport)
                         :error-output (transport-error-output transport)
                         (append (when (transport-env transport)
                                   (list :env (transport-env transport)
                                         :env-mode (transport-env-mode transport)))
                                 (when (transport-directory transport)
                                   (list :directory (transport-directory transport)))))))
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
  "The -32600 wire answer for MSG: its id when that is a number or a string,
else JSON null (:NULL; NIL would encode as false, #17)."
  (let ((id (and (hash-table-p msg) (gethash "id" msg))))
    (rpc-protocol:encode-error-response
     rpc-protocol:+invalid-request+ "Invalid Request"
     :id (if (or (realp id) (stringp id)) id :null))))

(defun %answer (handler msg)
  "Call HANDLER on the request or notification MSG. → the wire answer, or NIL
for a notification. RPC-ERROR → its code, message and data; any other ERROR
→ -32603 with the condition's text (the header's answer table)."
  (multiple-value-bind (id has-id) (gethash "id" msg)
    (flet ((fail (code text &optional data)
             (and has-id
                  (rpc-protocol:encode-error-response code text :id id :data data))))
      (handler-case
          (let ((result (let ((*stdio-rpc-request-id* (and has-id id)))
                          (funcall handler (gethash "method" msg) (gethash "params" msg)))))
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

(defun %parse-error ()
  (rpc-protocol:encode-error-response rpc-protocol:+parse-error+ "Parse error" :id :null))

(defun %serve-line (handler line &optional truncated)
  "The server's answer to one input LINE: a wire string, or NIL. TRUNCATED:
the line was cut short by EOF (0.3.1: no -32700 for it)."
  (when (%blank-line-p line)
    (return-from %serve-line nil))
  (let ((msg (handler-case (rpc-protocol:decode-message line)
               (error ()
                 (return-from %serve-line
                   (and (not truncated) (%parse-error)))))))
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

(defun %handle-incoming (transport output kind msg)
  "Run the handler for the incoming notification or request MSG (KIND), in
the calling thread, and write a request's answer."
  (ecase kind
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
         ;; A peer that went away cannot be answered; the answer is dropped.
         (handler-case (%write-json-line output wire transport)
           (error () nil)))))))

;;; 0.3.0: dispatch off the reader thread.

(defun %enqueue-locked (transport kind msg)
  "Queue MSG for the dispatcher thread; STATE-LOCK is held."
  (unless (transport-closed-by-user transport)
    (let ((cell (list (cons kind msg))))
      (if (transport-queue transport)
          (setf (cdr (transport-queue-tail transport)) cell)
          (setf (transport-queue transport) cell))
      (setf (transport-queue-tail transport) cell))
    (bt2:condition-broadcast (transport-state-cv transport))))

(defun %enqueue (transport kind msg)
  "Queue MSG for the dispatcher thread (under STATE-LOCK)."
  (bt2:with-lock-held ((transport-state-lock transport))
    (%enqueue-locked transport kind msg)))

(defun %take (transport)
  "Wait for the next queued (kind . message). → it, or NIL when the
dispatcher stops: the transport was closed by the user (the queue is
dropped), or the input reached EOF and the queue is empty. A taken item
counts as ACTIVE until %DONE."
  (let ((lock (transport-state-lock transport))
        (cv (transport-state-cv transport)))
    (bt2:with-lock-held (lock)
      (loop
        (cond ((transport-closed-by-user transport)
               (setf (transport-queue transport) nil
                     (transport-queue-tail transport) nil)
               (return nil))
              ((transport-queue transport)
               (incf (transport-active transport))
               (return (pop (transport-queue transport))))
              ((transport-closed transport) (return nil)))
        (bt2:condition-wait cv lock)))))

(defun %done (transport &optional thread)
  "A handler off the reader thread finished (THREAD: a request worker)."
  (bt2:with-lock-held ((transport-state-lock transport))
    (decf (transport-active transport))
    (when thread
      (setf (transport-workers transport) (delete thread (transport-workers transport))))
    (bt2:condition-broadcast (transport-state-cv transport))))

(defun %dispatcher-loop (transport output)
  (catch '%stop-worker                  ; CLOSE-STDIO-RPC-TRANSPORT's last resort
    (loop for item = (%take transport)
          while item
          do (unwind-protect
                  (handler-case (%handle-incoming transport output (car item) (cdr item))
                    (error () nil))
               (%done transport)))))

(defun %spawn-request-locked (transport output msg &optional barrier)
  ":CONCURRENT, STATE-LOCK held: run the request MSG on a thread of its own.
BARRIER (0.4.1): MSG is a barrier request; the messages read from now on are
held until its answer was written, then its worker releases them."
  (unless (transport-closed-by-user transport)
    (incf (transport-active transport))
    (when barrier
      (setf (transport-barrier-active transport) t))
    (let (thread)
      ;; The worker removes itself under STATE-LOCK, held here until it is
      ;; pushed, so it is never removed before it is added.
      (setf thread
            (bt2:make-thread
             (lambda ()
               (unwind-protect
                    (catch '%stop-worker
                      (handler-case (%handle-incoming transport output :request msg)
                        (error () nil)))
                 ;; The answer is written (or could not be): what came after
                 ;; the barrier goes now, before this worker counts as done.
                 (when barrier
                   (%release-held transport output))
                 (%done transport (bt2:current-thread))))
             :name "stdio-rpc request"))
      (push thread (transport-workers transport)))))

;;; 0.4.1: barriers (:CONCURRENT, see the header).

(defun %barrier-request-p (transport kind msg)
  (and (eq kind :request)
       (stdio-rpc-transport-barrier-methods transport)
       (member (gethash "method" msg) (stdio-rpc-transport-barrier-methods transport)
               :test #'equal)
       t))

(defun %dispatch-concurrent-locked (transport output kind msg)
  ":CONCURRENT, STATE-LOCK held: the request MSG to a thread of its own, the
notification MSG to the dispatcher, or MSG held behind the barrier that runs."
  (cond ((transport-closed-by-user transport) nil)
        ((transport-barrier-active transport)
         (setf (transport-held transport)
               (nconc (transport-held transport) (list (cons kind msg)))))
        ((eq kind :request)
         (%spawn-request-locked transport output msg (%barrier-request-p transport kind msg)))
        (t (%enqueue-locked transport kind msg))))

(defun %release-held (transport output)
  "A barrier's answer is out: dispatch the messages held behind it, oldest
first, until one of them is a barrier again (it holds the rest)."
  (bt2:with-lock-held ((transport-state-lock transport))
    (setf (transport-barrier-active transport) nil)
    (if (transport-closed-by-user transport)
        (setf (transport-held transport) nil)
        (loop while (and (transport-held transport)
                         (not (transport-barrier-active transport)))
              do (destructuring-bind (kind . msg) (pop (transport-held transport))
                   (%dispatch-concurrent-locked transport output kind msg))))
    (bt2:condition-broadcast (transport-state-cv transport))))

(defun %route-message (transport output msg)
  "Route one decoded object (or batch element) read by the client's reader.
→ the -32600 wire answer when MSG is not a valid envelope and the transport
serves (has ON-REQUEST), else NIL (0.3.1)."
  (let ((kind (%classify msg)))
    (ecase kind
      (:response (%route-response transport msg) nil)
      (:invalid (and (stdio-rpc-transport-on-request transport)
                     (%invalid-request msg)))
      ((:notification :request)
       (ecase (stdio-rpc-transport-dispatch transport)
         (:reader (%handle-incoming transport output kind msg))
         (:serial (%enqueue transport kind msg))
         (:concurrent (bt2:with-lock-held ((transport-state-lock transport))
                        (%dispatch-concurrent-locked transport output kind msg))))
       nil))))

(defun %route-line (transport output line &optional truncated)
  "Route one line read by the client's reader (see the header). TRUNCATED:
the line was cut short by EOF."
  (when (%blank-line-p line)
    (return-from %route-line nil))
  (let* ((serves (stdio-rpc-transport-on-request transport))
         (wire
           (let ((msg (handler-case (rpc-protocol:decode-message line)
                        (error ()
                          ;; 0.3.1: a peer that serves answers -32700.
                          (return-from %route-line
                            (when (and serves (not truncated))
                              (%write-answer transport output (%parse-error))))))))
             (cond ((hash-table-p msg) (%route-message transport output msg))
                   ((and (vectorp msg) (not (stringp msg)))
                    (if (zerop (length msg))
                        (and serves (%invalid-request nil))
                        (let ((answers (remove nil (map 'list (lambda (e) (%route-message transport output e))
                                                        msg))))
                          (and answers (format nil "[~{~a~^,~}]" answers)))))
                   ;; numbers, strings, true / false / null
                   (t (and serves (%invalid-request nil)))))))
    (when wire
      (%write-answer transport output wire))))

(defun %write-answer (transport output wire)
  "Write WIRE under the write lock. A peer that went away cannot be answered;
the answer is dropped."
  (handler-case (%write-json-line output wire transport)
    (error () nil)))

(defun %reader-loop (transport input output)
  (catch '%stop-reader                  ; CLOSE-STDIO-RPC-TRANSPORT's last resort
    (unwind-protect
         ;; A read failure (a stream closed under the reader, a decoding
         ;; error) is the peer closing: nothing escapes the thread function,
         ;; so the reader never reaches the debugger.
         (handler-case
             (loop
               (multiple-value-bind (line truncated) (read-line input nil :eof)
                 (when (eq line :eof) (return))
                 (handler-case (%route-line transport output line truncated)
                   (error () nil))))
           (error () nil))
      (%mark-closed transport))))

(defun %ensure-reader (transport input output)
  "Start the reader thread on INPUT unless it runs or the transport is closed."
  (bt2:with-lock-held ((transport-state-lock transport))
    (unless (or (transport-reader transport) (transport-closed transport))
      (unless (eq :reader (stdio-rpc-transport-dispatch transport))
        (setf (transport-dispatcher transport)
              (bt2:make-thread (lambda () (%dispatcher-loop transport output))
                               :name "stdio-rpc dispatcher")))
      (setf (transport-reader transport)
            (bt2:make-thread (lambda () (%reader-loop transport input output))
                             :name "stdio-rpc reader"))))
  transport)

(defun stdio-rpc-transport-wait-closed (transport &key timeout)
  "See the header (0.3.0). → T once the input is closed and no handler runs,
NIL when TIMEOUT seconds passed first."
  (multiple-value-call #'%ensure-reader transport (%ensure-io transport))
  (let ((lock (transport-state-lock transport))
        (cv (transport-state-cv transport))
        (deadline (and timeout
                       (+ (get-internal-real-time)
                          (round (* timeout internal-time-units-per-second))))))
    (bt2:with-lock-held (lock)
      (loop
        (when (and (or (transport-closed transport) (transport-closed-by-user transport))
                   (zerop (transport-active transport))
                   (or (transport-closed-by-user transport)
                       (and (null (transport-queue transport))
                            ;; 0.4.1: held behind a barrier
                            (null (transport-held transport)))))
          (return t))
        (if deadline
            (let ((remaining (/ (- deadline (get-internal-real-time))
                                internal-time-units-per-second)))
              (when (<= remaining 0) (return nil))
              (bt2:condition-wait cv lock :timeout remaining))
            (bt2:condition-wait cv lock))))))

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
  (when (eq (bt2:current-thread) (transport-reader transport))
    ;; Only the reader could read the response: waiting here never ends.
    (error 'rpc-protocol:rpc-error
           :message (format nil "~a ~s ~a" "rpc-call" method
                            "from a handler on the reader thread would deadlock; make the transport with :dispatch :serial or :concurrent")
           :code rpc-protocol:+internal-error+))
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
    (loop
      (multiple-value-bind (line truncated) (read-line in nil :eof)
        (when (eq line :eof) (return))
        (let ((wire (%serve-line handler line truncated)))
          (when wire
            (%write-json-line out wire transport))))))
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

(defun %close-bounded (stream seconds)
  "Close STREAM on a helper thread and wait for it up to SECONDS. → T when it
was closed in time. On Windows, closing a pipe while another thread's read is
pending blocks until that read ends (rpc-backend-stdio#10, #12)."
  (or (null stream)
      (%join-reader (bt2:make-thread (lambda () (%close-quietly stream))
                                     :name "stdio-rpc close")
                    seconds)))

(defun close-stdio-rpc-transport (transport &key force (timeout 5) (grace 3))
  "See the header (0.2.0: stdin first, TIMEOUT, the reader thread; 0.3.1:
GRACE seconds for EOF on the child's stdout once it exited)."
  (check-type grace (real 0))
  (when (bt2:with-lock-held ((transport-state-lock transport))
          (prog1 (shiftf (transport-closed-by-user transport) t)
            ;; The dispatcher stops taking queued messages.
            (bt2:condition-broadcast (transport-state-cv transport))))
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
    ;;    0.3.1: every wait is bounded; another process may hold the child's
    ;;    stdout, so EOF may never come (#12).
    (let ((input-closed nil))
      (when (%close-wakes-reader-p input)
        (%close-quietly input)
        (setf input-closed t))
      (when reader
        (unless (%join-reader reader (if process grace 0.5))
          (when process
            (warn 'stdio-rpc-pipe-held :process process :seconds grace))
          ;; Tried once: a close still blocked on its helper thread is not
          ;; tried again below.
          (%close-bounded input 1)
          (setf input-closed t)
          (unless (%join-reader reader 1)
            ;; Last resort: a read that close cannot wake (a foreign stream).
            ;; The reader unwinds to its CATCH, marking the transport closed.
            (handler-case
                (bt2:interrupt-thread reader (lambda () (throw '%stop-reader nil)))
              (error () nil))
            (%join-reader reader 1))))
      (unless input-closed
        (%close-bounded input 1)))
    ;; 4. Waiting calls get "peer closed".
    (%mark-closed transport)
    ;; 5. (0.3.0) The dispatcher and the request workers: their calls now
    ;;    fail and their answers cannot be written, so they end; one blocked
    ;;    on something else is unwound after a grace period.
    (%stop-workers transport))
  transport)

(defun %stop-workers (transport)
  (flet ((threads ()
           (bt2:with-lock-held ((transport-state-lock transport))
             (let ((d (transport-dispatcher transport)))
               (append (and d (list d)) (copy-list (transport-workers transport)))))))
    (let ((deadline (+ (get-internal-real-time) (* 2 internal-time-units-per-second))))
      (loop while (and (some #'bt2:thread-alive-p (threads))
                       (< (get-internal-real-time) deadline))
            do (sleep 0.01)))
    (dolist (thread (threads))
      (when (bt2:thread-alive-p thread)
        (handler-case (bt2:interrupt-thread thread (lambda () (throw '%stop-worker nil)))
          (error () nil))))
    (dolist (thread (threads))
      (%join-reader thread 1))))

(use-stdio-rpc-transport)
