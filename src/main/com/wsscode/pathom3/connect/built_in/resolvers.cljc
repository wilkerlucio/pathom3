(ns com.wsscode.pathom3.connect.built-in.resolvers
  (:require
    #?(:clj [com.wsscode.misc.core :as misc])
    [com.wsscode.pathom3.connect.operation :as pco]
    #?(:clj [com.wsscode.pathom3.format.eql :as pf.eql]))
  #?(:cljs
     (:require-macros
       [com.wsscode.pathom3.connect.built-in.resolvers])))

(defn attr-alias-name [from to]
  (symbol (str (munge (subs (str from) 1)) "->" (munge (subs (str to) 1)))))

(defn alias-resolver
  "Create a resolver that will convert property `from` to a property `to` with
  the same value. This only creates the alias in one direction."
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

(defn constantly-resolver
  "Create a simple resolver that always return `value` for `attribute`."
  ([attribute value]
   (constantly-resolver {::attribute attribute
                         :value      value}))
  ([{::keys [attribute sym] :keys [value]}]
   (let [sym (or sym (symbol (str (munge (subs (str attribute) 1)) "-constant")))]
     (pco/resolver sym
       {::pco/output [attribute]}
       (fn [_ _] {attribute value})))))

(defn single-attr-resolver
  "Apply fn `f` to input `from` and spits the result with the name `to`.

  `f` receives a single argument, which is the input value from `from`."
  [from to f]
  (let [sym (symbol (str (attr-alias-name from to) "-single-attr-transform"))]
    (pco/resolver sym
      {::pco/input  [from]
       ::pco/output [to]}
      (fn [_ input]
        {to (f (get input from))}))))

(defn single-attr-resolver2
  "Similar single-attr-resolver, but `f` receives two arguments, `env` and the input."
  [from to f]
  (let [sym (symbol (str (attr-alias-name from to) "-single-attr-transform"))]
    (pco/resolver sym
      {::pco/input  [from]
       ::pco/output [to]}
      (fn [env input]
        {to (f env (get input from))}))))

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

#?(:clj
   (defn system-env-resolver
     "Create resolver that exposes data available in system environment.

     Prefix will be used as the namespace for the environment variables, so for example
     if you want to access the $PATH variable made available from this helper:

        (let [m (psm/smart-map (pci/register (pbir/env-resolver \"env\")) {})]
          (:env/PATH m))

     Note that the exposed keys are the ones available when you call (system-env-resolver),
     if new keys are add you need to generate the resolver again to make it available.

     Clojure only."
     [prefix]
     (let [sym    (symbol "env-resolver" prefix)
           output (->> (System/getenv)
                       (keys)
                       (mapv #(keyword prefix %)))]
       (pco/resolver sym
         {::pco/output output}
         (fn [_ _]
           (misc/map-keys #(keyword prefix %) (System/getenv)))))))
