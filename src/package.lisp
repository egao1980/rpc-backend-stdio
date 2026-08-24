(defpackage #:rpc-backend-stdio
  (:use #:cl)
  (:export #:stdio-rpc-transport
           #:make-stdio-rpc-transport
           #:use-stdio-rpc-transport
           #:close-stdio-rpc-transport))

(in-package #:rpc-backend-stdio)
