;;;; Dogfood process-protocol × rpc-backend-stdio.
;;;;   sbcl --load scripts/live-stdio.lisp

(setf *debugger-hook*
      (lambda (c h)
        (declare (ignore h))
        (format *error-output* "~&live-stdio failed: ~a~%" c)
        (uiop:quit 1)))

(defun %here ()
  (uiop:pathname-directory-pathname
   (or *load-truename* *compile-file-truename* (uiop:getcwd))))

(defun %root ()
  (uiop:pathname-parent-directory-pathname (%here)))

(defun %workspace ()
  (uiop:pathname-parent-directory-pathname (%root)))

(dolist (name '("rpc-protocol" "rpc-backend-stdio"
                "process-protocol" "process-backend-uiop"))
  (pushnew (merge-pathnames (format nil "~a/" name) (%workspace))
           asdf:*central-registry* :test #'equal))

(asdf:load-system "rpc-backend-stdio")
(asdf:load-system "process-backend-uiop")

(defun fail (fmt &rest args)
  (apply #'format *error-output* (concatenate 'string "~&FAIL: " fmt "~%") args)
  (uiop:quit 1))

(let* ((lisp (or (uiop:argv0)
                 (first (uiop:raw-command-line-arguments))
                 "sbcl"))
       (server (namestring (merge-pathnames "echo-server.lisp" (%here))))
       (tx (rpc-backend-stdio:make-stdio-rpc-transport
            :command (list lisp "--noinform" "--non-interactive"
                           "--load" server))))
  (unwind-protect
       (progn
         (unless (equal "hi" (rpc-protocol:rpc-call "echo" "hi" :transport tx :id 1))
           (fail "echo"))
         (unless (= 3 (rpc-protocol:rpc-call "sum" #(1 2) :transport tx :id 2))
           (fail "sum")))
    (rpc-backend-stdio:close-stdio-rpc-transport tx)))

(format t "~&; live-stdio ok~%")
(uiop:quit 0)
