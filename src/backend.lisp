(in-package #:rpc-backend-stdio)

(defclass stdio-rpc-transport (rpc-protocol:rpc-transport)
  ((input :initarg :input :initform nil :accessor transport-input)
   (output :initarg :output :initform nil :accessor transport-output)
   (command :initarg :command :initform nil :accessor transport-command)
   (process :initform nil :accessor transport-process)
   (next-id :initform 0 :accessor transport-next-id)))

(defun make-stdio-rpc-transport (&key input output command)
  (make-instance 'stdio-rpc-transport
                 :input input :output output :command command))

(defun use-stdio-rpc-transport (&rest args &key &allow-other-keys)
  (setf rpc-protocol:*rpc-transport*
        (apply #'make-stdio-rpc-transport args)))

(defun close-stdio-rpc-transport (transport &key force)
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

(defun %write-json-line (stream string)
  (write-string string stream)
  (write-char #\newline stream)
  (finish-output stream))

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
      (%write-json-line out (rpc-protocol:encode-request method params :id id))
      (%raise-rpc (rpc-protocol:decode-message (%read-json-line in))))))

(defmethod rpc-protocol:backend-rpc-notify
    ((transport stdio-rpc-transport) method params)
  (multiple-value-bind (in out) (%ensure-io transport)
    (declare (ignore in))
    (%write-json-line out (rpc-protocol:encode-notification method params))
    t))

(defmethod rpc-protocol:backend-rpc-serve
    ((transport stdio-rpc-transport) handler &key)
  (multiple-value-bind (in out) (%ensure-io transport)
    (loop for line = (read-line in nil :eof)
          until (eq line :eof)
          do (let ((wire (%handle-message handler (rpc-protocol:decode-message line))))
               (when wire
                 (%write-json-line out wire)))))
  transport)

(use-stdio-rpc-transport)
