(in-package #:rpc-backend-stdio/tests)

;;; 0.4.0: :ENV, :ENV-MODE and :DIRECTORY for the :COMMAND child
;;; (egao1980/mcp-backend-stdio#7).

#+sbcl
(defun %probe-child-argv ()
  "This SBCL, no init files, serving \"probe\" → [RBS_PROBE or null, RBS_GONE
or null, the child's working directory]."
  (let ((dirs '()))
    (dolist (name (asdf:already-loaded-systems))
      (let ((dir (ignore-errors (asdf:system-source-directory name))))
        (when dir (pushnew (uiop:native-namestring dir) dirs :test #'string=))))
    (append
     (list (uiop:native-namestring sb-ext:*runtime-pathname*)
           "--core" (uiop:native-namestring sb-ext:*core-pathname*) "--noinform"
           "--non-interactive" "--no-sysinit" "--no-userinit")
     (loop for form in
           (list "(require :asdf)"
                 (format nil "(asdf:initialize-source-registry '~s)"
                         `(:source-registry ,@(mapcar (lambda (d) (list :directory d)) dirs)
                                            :ignore-inherited-configuration))
                 "(let ((*standard-output* (make-broadcast-stream))) (asdf:load-system \"rpc-backend-stdio\"))"
                 "(let ((tx (rpc-backend-stdio:make-stdio-rpc-transport))) (rpc-protocol:rpc-serve (lambda (m p) (declare (ignore m p)) (vector (or (uiop:getenv \"RBS_PROBE\") :null) (or (uiop:getenv \"RBS_GONE\") :null) (uiop:native-namestring (uiop:getcwd)))) :transport tx))")
           append (list "--eval" form)))))

(defun %same-directory-p (a b)
  (equal (ignore-errors (truename (uiop:ensure-directory-pathname a)))
         (ignore-errors (truename (uiop:ensure-directory-pathname b)))))

(defun %probe (&rest keys)
  "Start the probe child with KEYS, call probe, close. → the result as a list."
  (let ((tx (apply #'rpc-backend-stdio:make-stdio-rpc-transport
                   :command (%probe-child-argv) :error-output nil keys)))
    (unwind-protect
         (coerce (rpc-protocol:rpc-call "probe" nil :transport tx :timeout 120) 'list)
      (rpc-backend-stdio:close-stdio-rpc-transport tx :force t))))

(defun %null-value-p (v) (member v '(nil :null)))

(deftest command-env-and-directory
  #-sbcl (skip "the child is started with SBCL")
  #+sbcl
  (let ((dir (uiop:ensure-directory-pathname
              (merge-pathnames (format nil "rbs-env-~36r/" (random (expt 36 8) (make-random-state t)))
                               (uiop:temporary-directory)))))
    (ensure-directories-exist dir)
    (unwind-protect
         (destructuring-bind (probe gone cwd)
             (%probe :env '(("RBS_PROBE" . "from-parent") ("RBS_GONE"))
                     :directory dir)
           (ok (equal "from-parent" probe) ":env reaches the child")
           (ok (%null-value-p gone) "a NIL value is unset in the child")
           (ok (%same-directory-p cwd dir) (format nil ":directory is the child's cwd (~a)" cwd)))
      (uiop:delete-directory-tree dir :validate t :if-does-not-exist :ignore))))

(deftest command-env-default-inherits
  #-sbcl (skip "the child is started with SBCL")
  #+sbcl
  (destructuring-bind (probe gone cwd) (%probe)
    (ok (%null-value-p probe) "no :env → nothing added")
    (ok (%null-value-p gone))
    (ok (%same-directory-p cwd (uiop:getcwd)) "no :directory → the parent's cwd")))

(deftest command-env-mode-checked
  (ok (signals (rpc-backend-stdio:make-stdio-rpc-transport :command '("x") :env-mode :bogus)
               'type-error)))
