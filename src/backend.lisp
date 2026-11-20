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
           :documentation "True once the input reached EOF or the transport was closed.")))

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

(defun make-stdio-rpc-transport (&key input output command (error-output :interactive)
                                      on-notification on-request)
  "A transport on INPUT / OUTPUT (default *STANDARD-INPUT* / *STANDARD-OUTPUT*
when used), or on the pipes of COMMAND launched on first use. ON-NOTIFICATION
or ON-REQUEST make it a client whose reader thread starts at once (see the
header)."
  (make-instance 'stdio-rpc-transport
                 :input input :output output :command command
                 :error-output error-output
                 :on-notification on-notification :on-request on-request))

(defun use-stdio-rpc-transport (&rest args &key &allow-other-keys)
  (setf rpc-protocol:*rpc-transport*
        (apply #'make-stdio-rpc-transport args)))

(defun close-stdio-rpc-transport (transport &key force (timeout 5))
  "See the header (0.2.0: stdin first, TIMEOUT, the reader thread)."
  (declare (ignore timeout))
  (let ((proc (transport-process transport)))
    (when proc
      (ignore-errors (process-protocol:kill proc :force force))
      (ignore-errors (process-protocol:wait proc))
      (setf (transport-process transport) nil)))
  transport)

(defun %ensure-io (transport)
  (when (and (transport-command transport)
             (null (transport-process transport)))
    (unless process-protocol:*process-backend*
      (error 'rpc-protocol:rpc-error
             :message "*process-backend* is nil — load process-backend-uiop"
             :code rpc-protocol:+internal-error+))
    (let ((handle (process-protocol:launch (transport-command transport))))
      (setf (transport-process transport) handle
            (transport-input transport) (process-protocol:stdout handle)
            (transport-output transport) (process-protocol:stdin handle))))
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

(defun %read-json-line (stream)
  (let ((line (read-line stream nil :eof)))
    (if (eq line :eof)
        (error 'rpc-protocol:rpc-error
               :message "stdio RPC peer closed"
               :code rpc-protocol:+internal-error+)
        line)))

(defun %raise-rpc (msg)
  (let ((err (gethash "error" msg)))
    (if err
        (error 'rpc-protocol:rpc-error
               :code (or (gethash "code" err) rpc-protocol:+internal-error+)
               :message (gethash "message" err)
               :data (gethash "data" err))
        (gethash "result" msg))))

(defun %handle-message (handler msg)
  (let ((method (gethash "method" msg))
        (params (gethash "params" msg))
        (id (gethash "id" msg)))
    (unless method
      (return-from %handle-message
        (when id
          (rpc-protocol:encode-error-response
           rpc-protocol:+invalid-request+ "missing method" :id id))))
    (handler-case
        (let ((result (funcall handler method params)))
          (when id
            (rpc-protocol:encode-response result :id id)))
      (rpc-protocol:rpc-error (c)
        (when id
          (rpc-protocol:encode-error-response
           (rpc-protocol:rpc-error-code c)
           (or (rpc-protocol:rpc-error-message c) "rpc error")
           :id id :data (rpc-protocol:rpc-error-data c))))
      (error (c)
        (when id
          (rpc-protocol:encode-error-response
           rpc-protocol:+internal-error+ (format nil "~a" c) :id id))))))

(defmethod rpc-protocol:backend-rpc-call
    ((transport stdio-rpc-transport) method params &key timeout id)
  (declare (ignore timeout))
  (multiple-value-bind (in out) (%ensure-io transport)
    (let ((id (or id (incf (transport-next-id transport)))))
      (%write-json-line out (rpc-protocol:encode-request method params :id id) transport)
      (%raise-rpc (rpc-protocol:decode-message (%read-json-line in))))))

(defmethod rpc-protocol:backend-rpc-notify
    ((transport stdio-rpc-transport) method params)
  (multiple-value-bind (in out) (%ensure-io transport)
    (declare (ignore in))
    (%write-json-line out (rpc-protocol:encode-notification method params) transport)
    t))

(defmethod rpc-protocol:backend-rpc-serve
    ((transport stdio-rpc-transport) handler &key)
  (multiple-value-bind (in out) (%ensure-io transport)
    (loop for line = (read-line in nil :eof)
          until (eq line :eof)
          do (let ((wire (%handle-message handler (rpc-protocol:decode-message line))))
               (when wire
                 (%write-json-line out wire transport)))))
  transport)

(use-stdio-rpc-transport)
