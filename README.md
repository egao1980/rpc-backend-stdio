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

### Peer mode: handlers that call back (0.3.0)

A client-mode transport (one with `:on-request` / `:on-notification`) runs its
handlers where `:dispatch` says:

| `:dispatch` | requests | notifications |
|---|---|---|
| `:reader` (default, 0.2.0) | on the reader thread; an `rpc-call` from there signals at once (it could never be answered) | reader thread, in order |
| `:serial` | one dispatcher thread, one at a time, arrival order | same thread, interleaved in arrival order |
| `:concurrent` | a thread per request, at once | one dispatcher thread, in order |

Off the reader, a handler may `rpc-call` its peer and wait (ACP:
`session/prompt` → `session/request_permission`); responses are always routed
by the reader. Notifications are always handled one at a time, in arrival
order. `*stdio-rpc-request-id*` is the running request's id.
`close-stdio-rpc-transport` drops queued messages, fails the handlers' pending
calls with "peer closed" and joins the workers;
`stdio-rpc-transport-wait-closed` blocks until the peer's EOF and the handlers
are done.

```lisp
(make-stdio-rpc-transport :on-request #'handle :on-notification #'note
                          :dispatch :concurrent)
```

CI: canned [`cl-repository`](https://github.com/egao1980/cl-repository) (`test-system.yml` / `setup-client` + `ci`). Deps from `ghcr.io/egao1980/cl-systems`.

## License

MIT
