(defsystem "rpc-backend-stdio"
  :version "0.2.0"
  :description "stdio (newline JSON-RPC) transport for rpc-protocol: a server loop that answers parse and request errors, a client that demultiplexes responses, notifications and server requests on a reader thread, and in-memory pipe pairs for tests"
  :author "egao1980"
  :license "MIT"
  :depends-on ("rpc-protocol" "rpc-protocol-json" "process-protocol" "bordeaux-threads"
               "trivial-gray-streams")
  :properties (:cl-repo (:ci (:with ("dissect"))))
  :serial t
  :pathname "src"
  :components ((:file "package")
               (:file "backend")
               (:file "pipe"))
  :in-order-to ((test-op (test-op "rpc-backend-stdio/tests"))))

(defsystem "rpc-backend-stdio/tests"
  :depends-on ("rpc-backend-stdio" "process-backend-uiop" "rove" "bordeaux-threads")
  :pathname "tests"
  :serial t
  :components ((:file "package")
               (:file "backend-test")
               ;; 0.2.0
               (:file "peer-test"))
  :perform (test-op (o c)
             (unless (symbol-call :rove :run c)
               (error "tests failed for ~A" (component-name c)))))
