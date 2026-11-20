(in-package #:rpc-backend-stdio)

;;; 0.2.0 (contract): in-memory pipes, so a server and a client can talk in
;;; one image without file descriptors (tests, an SDK hosting a server
;;; in-process). Portable: trivial-gray-streams, no SB-UNIX.
;;;
;;; MAKE-STDIO-PIPE &key (CAPACITY 65536) → (values input output)
;;;   Two character streams over one shared buffer. What is written to
;;;   OUTPUT is read from INPUT, in order. READ-CHAR / READ-LINE /
;;;   PEEK-CHAR / READ-CHAR-NO-HANG and LISTEN work on INPUT; WRITE-CHAR /
;;;   WRITE-STRING / WRITE-LINE / FINISH-OUTPUT / FORCE-OUTPUT on OUTPUT.
;;;   A read blocks until a character is there or OUTPUT is closed; once
;;;   OUTPUT is closed and the buffer is empty, reads see EOF. A write blocks
;;;   while CAPACITY characters are unread (back-pressure, as a pipe), and
;;;   writing after INPUT was closed signals STREAM-ERROR (a broken pipe).
;;;   CLOSE on either end wakes every blocked reader and writer. Any number
;;;   of threads may write (each WRITE-STRING call is atomic: its characters
;;;   are not interleaved with another thread's); one thread reads at a
;;;   time. OPEN-STREAM-P follows CLOSE. INPUT-STREAM-P / OUTPUT-STREAM-P are
;;;   true for INPUT / OUTPUT only. STREAM-ELEMENT-TYPE is CHARACTER.
;;;
;;; MAKE-STDIO-RPC-PAIR &key ON-NOTIFICATION ON-REQUEST
;;;   → (values server client): two STDIO-RPC-TRANSPORTs over two pipes,
;;;   the server reading what the client writes and the client reading what
;;;   the server writes. ON-NOTIFICATION / ON-REQUEST go to the CLIENT (its
;;;   reader starts at once when one is given). Run RPC-SERVE on SERVER in a
;;;   thread of its own. CLOSE-STDIO-RPC-TRANSPORT on the client closes the
;;;   pipe the server reads, so the server's RPC-SERVE returns (EOF); on the
;;;   server it closes the pipe the client reads, so waiting calls signal
;;;   "peer closed".

(defun make-stdio-pipe (&key (capacity 65536))
  "→ (values input output), an in-memory character pipe (see the header)."
  (declare (ignore capacity))
  (%not-implemented 'make-stdio-pipe))

(defun make-stdio-rpc-pair (&key on-notification on-request)
  "→ (values server client) over two in-memory pipes (see the header)."
  (declare (ignore on-notification on-request))
  (%not-implemented 'make-stdio-rpc-pair))
