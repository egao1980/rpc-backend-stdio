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

CI: canned [`cl-repository`](https://github.com/egao1980/cl-repository) (`test-system.yml` / `setup-client` + `ci`). Deps from `ghcr.io/egao1980/cl-systems`.

## License

MIT
