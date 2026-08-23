(defsystem "rpc-backend-stdio"
  :version "0.1.0"
  :description "stdio (newline JSON-RPC) transport for rpc-protocol"
  :author "egao1980"
  :license "MIT"
  :depends-on ("rpc-protocol" "process-protocol")
  :serial t
  :pathname "src"
  :components ((:file "package")
               (:file "backend"))
  :in-order-to ((test-op (test-op "rpc-backend-stdio/tests"))))

(defsystem "rpc-backend-stdio/tests"
  :depends-on ("rpc-backend-stdio" "rove")
  :pathname "tests"
  :serial t
  :components ((:file "package")
               (:file "backend-test"))
  :perform (test-op (o c)
             (unless (symbol-call :rove :run c)
               (error "tests failed for ~A" (component-name c)))))
