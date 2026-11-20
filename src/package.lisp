(defpackage #:rpc-backend-stdio
  (:use #:cl)
  (:export #:stdio-rpc-transport
           #:make-stdio-rpc-transport
           #:use-stdio-rpc-transport
           #:close-stdio-rpc-transport
           ;; 0.2.0
           #:stdio-rpc-transport-on-notification
           #:stdio-rpc-transport-on-request
           #:stdio-rpc-transport-process
           #:stdio-rpc-timeout
           #:stdio-rpc-timeout-method
           #:stdio-rpc-timeout-seconds
           #:make-stdio-pipe
           #:make-stdio-rpc-pair))
