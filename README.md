# rpc-backend-stdio

stdio (newline JSON-RPC) transport for rpc-protocol.

Part of [cl-stack](https://github.com/egao1980/cl-stack) agent-wire ([brief](https://github.com/egao1980/cl-stack/blob/main/docs/capabilities/agent-wire.md)).

```lisp
(asdf:load-system "rpc-backend-stdio")

(let ((tx (rpc-backend-stdio:make-stdio-rpc-transport
           :command '("sbcl" "--script" "scripts/echo-server.lisp"))))
  (rpc-protocol:rpc-call "echo" "hi" :transport tx))
```

`sbcl --load scripts/live-stdio.lisp` (process-protocol × UIOP).

CI: `setup-client` + `setup-roswell` + `scripts/ci-install.lisp` / `ci-test.lisp` (OCI only, no Quicklisp).

## License

MIT
