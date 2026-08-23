(in-package #:rpc-backend-stdio)

(defclass stdio-rpc-transport (rpc-protocol:rpc-transport) ())

(defun make-stdio-rpc-transport ()
  (make-instance 'stdio-rpc-transport))

(defun use-stdio-rpc-transport ()
  (setf rpc-protocol:*rpc-transport* (make-stdio-rpc-transport)))

(defmethod rpc-protocol:backend-rpc-call ((transport stdio-rpc-transport) method params &key timeout id)
  (declare (ignore timeout id))
  (error 'rpc-protocol:rpc-error
         :message "rpc-backend-stdio: backend-rpc-call not implemented"
         :code rpc-protocol:+internal-error+))

(defmethod rpc-protocol:backend-rpc-notify ((transport stdio-rpc-transport) method params)
  (declare (ignore method params))
  (error 'rpc-protocol:rpc-error
         :message "rpc-backend-stdio: backend-rpc-notify not implemented"
         :code rpc-protocol:+internal-error+))

(defmethod rpc-protocol:backend-rpc-serve ((transport stdio-rpc-transport) handler &key)
  (declare (ignore handler))
  (error 'rpc-protocol:rpc-error
         :message "rpc-backend-stdio: backend-rpc-serve not implemented"
         :code rpc-protocol:+internal-error+))
