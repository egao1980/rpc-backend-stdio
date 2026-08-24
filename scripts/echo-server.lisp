;;;; JSON-RPC echo/sum over stdin/stdout. Used by live-stdio.lisp.

(defun %here ()
  (uiop:pathname-directory-pathname
   (or *load-truename* *compile-file-truename* (uiop:getcwd))))

(pushnew (uiop:pathname-parent-directory-pathname (%here))
         asdf:*central-registry* :test #'equal)
(asdf:load-system "rpc-backend-stdio")

(rpc-backend-stdio:use-stdio-rpc-transport)
(rpc-protocol:rpc-serve
 (lambda (method params)
   (cond
     ((equal method "echo") params)
     ((equal method "sum") (+ (elt params 0) (elt params 1)))
     (t (error 'rpc-protocol:rpc-method-not-found)))))
