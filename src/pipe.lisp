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
;;; MAKE-STDIO-RPC-PAIR &key ON-NOTIFICATION ON-REQUEST DISPATCH (0.3.0)
;;;   → (values server client): two STDIO-RPC-TRANSPORTs over two pipes,
;;;   the server reading what the client writes and the client reading what
;;;   the server writes. ON-NOTIFICATION / ON-REQUEST / DISPATCH go to the CLIENT (its
;;;   reader starts at once when one is given). Run RPC-SERVE on SERVER in a
;;;   thread of its own. CLOSE-STDIO-RPC-TRANSPORT on the client closes the
;;;   pipe the server reads, so the server's RPC-SERVE returns (EOF); on the
;;;   server it closes the pipe the client reads, so waiting calls signal
;;;   "peer closed".

(define-condition %pipe-broken (stream-error) ()
  (:report (lambda (c s)
             (format s "write to an in-memory pipe whose input end is closed: ~s"
                     (stream-error-stream c))))
  (:documentation "A broken pipe: OUTPUT written after INPUT was closed."))

(defstruct (%pipe (:constructor %make-pipe
                      (capacity &aux (buffer (make-string capacity)))))
  "The buffer both ends share: a ring of CAPACITY characters. LOCK guards
every slot; CV is broadcast on every change (data, space, a close).
WRITE-MUTEX is held for a whole WRITE-STRING, so one call's characters are
never interleaved with another writer's, even while it waits for space."
  (capacity 0 :type (integer 1))
  (buffer "" :type simple-string)
  (start 0 :type fixnum)
  (count 0 :type fixnum)
  (writer-closed nil)
  (reader-closed nil)
  (lock (bt2:make-lock :name "stdio pipe"))
  (cv (bt2:make-condition-variable :name "stdio pipe"))
  (write-mutex (bt2:make-lock :name "stdio pipe writer")))

(defclass %pipe-stream (trivial-gray-streams:fundamental-character-stream)
  ((pipe :initarg :pipe :reader %stream-pipe)))

(defclass %pipe-input (%pipe-stream trivial-gray-streams:fundamental-character-input-stream)
  ((unread :initform nil :accessor %pipe-unread
           :documentation "The character UNREAD-CHAR gave back, or NIL.")))

(defclass %pipe-output (%pipe-stream trivial-gray-streams:fundamental-character-output-stream)
  ())

(defmethod stream-element-type ((stream %pipe-stream))
  'character)

(defmethod input-stream-p ((stream %pipe-output)) nil)
(defmethod output-stream-p ((stream %pipe-input)) nil)

(defmethod %close-wakes-reader-p ((stream %pipe-input)) t)

;;; Reading (one reader thread at a time).

(defun %pipe-pop (pipe)
  "Under the lock, with COUNT > 0: take the oldest character."
  (let ((char (schar (%pipe-buffer pipe) (%pipe-start pipe))))
    (setf (%pipe-start pipe) (mod (1+ (%pipe-start pipe)) (%pipe-capacity pipe)))
    (decf (%pipe-count pipe))
    (bt2:condition-broadcast (%pipe-cv pipe))   ; space for a waiting writer
    char))

(defun %pipe-read (stream hang)
  "→ a character, :EOF, or (HANG false and nothing there) NIL."
  (let ((unread (%pipe-unread stream)))
    (when unread
      (setf (%pipe-unread stream) nil)
      (return-from %pipe-read unread)))
  (let ((pipe (%stream-pipe stream)))
    (bt2:with-lock-held ((%pipe-lock pipe))
      (loop
        (cond ((plusp (%pipe-count pipe)) (return (%pipe-pop pipe)))
              ((or (%pipe-writer-closed pipe) (%pipe-reader-closed pipe)) (return :eof))
              ((not hang) (return nil)))
        ;; The timeout only bounds the cost of a missed wakeup.
        (bt2:condition-wait (%pipe-cv pipe) (%pipe-lock pipe) :timeout 1)))))

(defmethod trivial-gray-streams:stream-read-char ((stream %pipe-input))
  (%pipe-read stream t))

(defmethod trivial-gray-streams:stream-read-char-no-hang ((stream %pipe-input))
  (%pipe-read stream nil))

(defmethod trivial-gray-streams:stream-unread-char ((stream %pipe-input) char)
  (setf (%pipe-unread stream) char)
  nil)

(defmethod trivial-gray-streams:stream-peek-char ((stream %pipe-input))
  (let ((char (%pipe-read stream t)))
    (unless (eq char :eof)
      (setf (%pipe-unread stream) char))
    char))

(defmethod trivial-gray-streams:stream-listen ((stream %pipe-input))
  (or (and (%pipe-unread stream) t)
      (let ((pipe (%stream-pipe stream)))
        (bt2:with-lock-held ((%pipe-lock pipe))
          (plusp (%pipe-count pipe))))))

(defmethod trivial-gray-streams:stream-read-line ((stream %pipe-input))
  (let ((line (make-string-output-stream)))
    (loop
      (let ((char (%pipe-read stream t)))
        (cond ((eq char :eof)
               (return (values (get-output-stream-string line) t)))
              ((char= char #\Newline)
               (return (values (get-output-stream-string line) nil)))
              (t (write-char char line)))))))

(defmethod trivial-gray-streams:stream-clear-input ((stream %pipe-input))
  (setf (%pipe-unread stream) nil)
  (let ((pipe (%stream-pipe stream)))
    (bt2:with-lock-held ((%pipe-lock pipe))
      (setf (%pipe-start pipe) 0 (%pipe-count pipe) 0)
      (bt2:condition-broadcast (%pipe-cv pipe))))
  nil)

;;; Writing (any number of threads).

(defmethod trivial-gray-streams:stream-write-string
    ((stream %pipe-output) string &optional (start 0) end)
  (let ((pipe (%stream-pipe stream))
        (end (or end (length string))))
    (bt2:with-lock-held ((%pipe-write-mutex pipe))
      (bt2:with-lock-held ((%pipe-lock pipe))
        (loop for i from start below end
              do (loop
                   (when (or (%pipe-reader-closed pipe) (%pipe-writer-closed pipe))
                     (error '%pipe-broken :stream stream))
                   (when (< (%pipe-count pipe) (%pipe-capacity pipe))
                     (return))
                   ;; Full: let the reader drain. CONDITION-WAIT releases LOCK
                   ;; only; WRITE-MUTEX stays held, so other writers queue.
                   (bt2:condition-broadcast (%pipe-cv pipe))
                   (bt2:condition-wait (%pipe-cv pipe) (%pipe-lock pipe) :timeout 1))
                 (setf (schar (%pipe-buffer pipe)
                              (mod (+ (%pipe-start pipe) (%pipe-count pipe))
                                   (%pipe-capacity pipe)))
                       (char string i))
                 (incf (%pipe-count pipe)))
        (bt2:condition-broadcast (%pipe-cv pipe)))))
  string)

(defmethod trivial-gray-streams:stream-write-char ((stream %pipe-output) char)
  (trivial-gray-streams:stream-write-string stream (string char))
  char)

(defmethod trivial-gray-streams:stream-line-column ((stream %pipe-output))
  nil)

(defmethod trivial-gray-streams:stream-finish-output ((stream %pipe-output))
  nil)

(defmethod trivial-gray-streams:stream-force-output ((stream %pipe-output))
  nil)

;;; Closing. CLOSE takes only LOCK, never WRITE-MUTEX: a writer blocked on a
;;; full pipe holds that, and the close is what wakes it.

(defun %pipe-close-side (stream side)
  (let ((pipe (%stream-pipe stream)))
    (bt2:with-lock-held ((%pipe-lock pipe))
      (ecase side
        (:reader (setf (%pipe-reader-closed pipe) t))
        (:writer (setf (%pipe-writer-closed pipe) t)))
      (bt2:condition-broadcast (%pipe-cv pipe)))))

(defmethod close ((stream %pipe-input) &key abort)
  (declare (ignore abort))
  (%pipe-close-side stream :reader)
  (call-next-method))

(defmethod close ((stream %pipe-output) &key abort)
  (declare (ignore abort))
  (%pipe-close-side stream :writer)
  (call-next-method))

(defun make-stdio-pipe (&key (capacity 65536))
  "→ (values input output), an in-memory character pipe (see the header)."
  (check-type capacity (integer 1))
  (let ((pipe (%make-pipe capacity)))
    (values (make-instance '%pipe-input :pipe pipe)
            (make-instance '%pipe-output :pipe pipe))))

(defun make-stdio-rpc-pair (&key on-notification on-request (dispatch :reader))
  "→ (values server client) over two in-memory pipes (see the header)."
  (multiple-value-bind (server-in client-out) (make-stdio-pipe)
    (multiple-value-bind (client-in server-out) (make-stdio-pipe)
      (values (make-stdio-rpc-transport :input server-in :output server-out)
              (make-stdio-rpc-transport :input client-in :output client-out
                                        :on-notification on-notification
                                        :on-request on-request
                                        :dispatch dispatch)))))
