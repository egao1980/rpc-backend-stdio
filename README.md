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

### 0.3.1

- A transport with `:on-request` answers lines it cannot use as the server
  loop does: not JSON → -32700 `Parse error` (`"id": null`), not a valid
  envelope → -32600 `Invalid Request`, and reads on. Without `:on-request`
  they are dropped. A last line cut short by EOF is never answered.
- `close-stdio-rpc-transport … :grace 3`: once the child exited, EOF on its
  stdout is waited for `grace` seconds at most. If another process still holds
  the pipe, `stdio-rpc-pipe-held` (a `warning`) is signalled, the stream is
  closed and close returns.

### 0.3.2

- `-32700` / `-32600` answers carry `"id":null` on the wire (0.3.1 wrote
  `"id":false`).
- `stdio-rpc-peer-closed`: the "peer closed" condition, a subclass of
  `rpc-protocol:rpc-error` with code -32603 (existing handlers still match).
- `:class` on `make-stdio-rpc-transport` / `make-stdio-rpc-pair`: instantiate a
  subclass of `stdio-rpc-transport`.

CI: canned [`cl-repository`](https://github.com/egao1980/cl-repository) (`test-system.yml` / `setup-client` + `ci`). Deps from `ghcr.io/egao1980/cl-systems`.

## License

MIT

### The child's environment and directory (0.4.0)

`:command` children take `:env` (entries `(name . value)` or `"NAME=VALUE"`),
`:env-mode` (`:merge`, the default: over the inherited environment, a NIL value
unsets; `:replace`: exactly `:env`) and `:directory`, passed to
`process-protocol:launch`.

```lisp
(make-stdio-rpc-transport :command '("my-server") :env '(("TOKEN" . "x"))
                          :directory #p"/srv/project/" :error-output log-path)
```

### Barrier requests (0.4.1)

Under `:dispatch :concurrent` every request runs on a thread of its own, so a
request a peer pipelines after `initialize` (without waiting for its answer)
could run before `initialize` was handled. `:barrier-methods` names the
requests that must finish first: the requests and notifications read after
one of them are held until its answer has been written, then dispatched in
arrival order. Responses are routed as they arrive, so the barrier's handler
may still call the peer. `:serial` and `:reader` are in order already.

```lisp
(make-stdio-rpc-transport :input in :output out :dispatch :concurrent
                          :on-request #'handle :barrier-methods '("initialize"))
```
