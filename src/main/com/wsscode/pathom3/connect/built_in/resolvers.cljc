(ns com.wsscode.pathom3.connect.built-in.resolvers
  (:require
    [com.wsscode.pathom3.connect.operation :as pco]
    #?(:clj [com.wsscode.pathom3.format.eql :as pf.eql]))
  #?(:cljs
     (:require-macros
       [com.wsscode.pathom3.connect.built-in.resolvers])))

(defn attr-alias-name [from to]
  (symbol (str (munge (subs (str from) 1)) "->" (munge (subs (str to) 1)))))

(defn alias-resolver
  "Create a resolver that will convert property `from` to a property `to` with
  the same value. This only creates the alias in one direction"
  [from to]
  (pco/resolver (attr-alias-name from to)
    {::pco/input  [from]
     ::pco/output [to]}
    (fn [_ input] {to (get input from)})))

(defn alias-resolver2
  "Like alias-resolver, but returns a vector containing the alias in both directions."
  [attribute-a attribute-b]
  [(alias-resolver attribute-a attribute-b)
   (alias-resolver attribute-b attribute-a)])

#?(:clj
   (defmacro edn-file-resolver
     "Creates a resolver to provide data loaded from a file.

     This is a macro and the file will be read at compilation time, this way it
     can work on both Clojure and Clojurescript, without a need for async processing."
     [file-path]
     (let [data   (read-string (slurp file-path))
           sym    (symbol "edn-file-resolver" (munge file-path))
           output (pf.eql/data->shape data)]
       `(pco/resolver '~sym
          {::pco/output ~output}
          (fn ~'[_ _] ~data)))))
