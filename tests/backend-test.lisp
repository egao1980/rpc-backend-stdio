(in-package #:rpc-backend-stdio/tests)

(deftest transport-class
  (ok (typep (rpc-backend-stdio:make-stdio-rpc-transport) 'rpc-backend-stdio:stdio-rpc-transport)))
