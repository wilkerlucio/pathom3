(ns com.wsscode.pathom3.connect.built-in.resolvers
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    #?(:clj [com.wsscode.misc.coll :as coll])
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.format.eql :as pf.eql])
  #?(:cljs
     (:require-macros
       [com.wsscode.pathom3.connect.built-in.resolvers])))

(>def ::entity-table (s/map-of any? map?))

(defn attr-munge [attr]
  (munge (subs (str attr) 1)))

(defn attr-alias-resolver-name [from to]
  (symbol (str (attr-munge from) "->" (attr-munge to))))

(defn alias-resolver
  "Create a resolver that will convert property `from` to a property `to` with
  the same value. This only creates the alias in one direction."
  [from to]
  (pco/resolver (attr-alias-resolver-name from to)
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
   (let [resolver-name (or sym (symbol (str (munge (subs (str attribute) 1)) "-constant")))]
     (pco/resolver resolver-name
       {::pco/output [attribute]}
       (fn [_ _] {attribute value})))))

(defn single-attr-resolver
  "Apply fn `f` to input `from` and spits the result with the name `to`.

  `f` receives a single argument, which is the input value from `from`."
  [from to f]
  (let [resolver-name (symbol (str (attr-alias-resolver-name from to) "-single-attr-transform"))]
    (pco/resolver resolver-name
      {::pco/input  [from]
       ::pco/output [to]}
      (fn [_ input]
        {to (f (get input from))}))))

(defn single-attr-resolver2
  "Similar single-attr-resolver, but `f` receives two arguments, `env` and the input."
  [from to f]
  (let [resolver-name (symbol (str (attr-alias-resolver-name from to) "-single-attr-transform"))]
    (pco/resolver resolver-name
      {::pco/input  [from]
       ::pco/output [to]}
      (fn [env input]
        {to (f env (get input from))}))))

(defn table-output
  "For a given static table, compute the accumulated output query of the entity values."
  [table]
  (let [[{:keys [output]}] (pf.eql/data->query {:output (vec (vals table))})]
    output))

(>defn static-table-resolver
  "Exposes data for entities, indexes by attr-key. This is a simple way to extend/provide
  data for entities using simple Clojure maps. Example:

      (def registry
        [(pbir/static-table-resolver `song-names :song/id
           {1 {:song/name \"Marchinha Psicotica de Dr. Soup\"}
            2 {:song/name \"There's Enough\"}})

         (pbir/static-table-resolver `song-analysis :song/id
           {1 {:song/duration 280 :song/tempo 98}
            2 {:song/duration 150 :song/tempo 130}})])

      (let [sm (psm/smart-map (pci/register registry)
                 {:song/id 1})]
        (select-keys sm [:song/id :song/name :song/duration]))
      ; => #:song{:id 1, :name \"Marchinha Psicotica de Dr. Soup\", :duration 280}

  In this example, we create two different tables that provides data about songs, the
  entities are related by the keys on the table, the `attr-key` says what's the property
  name to be used to related the data, in this case we use `:song/id` on both, so they
  get connected by it.
  "
  [resolver-name attr-key table]
  [::pco/op-name ::p.attr/attribute ::entity-table
   => ::pco/resolver]
  (let [output (table-output table)]
    (pco/resolver resolver-name
      {::pco/input  [attr-key]
       ::pco/output output}
      (fn [_ input]
        (let [id (get input attr-key)]
          (get table id))))))

(>defn attribute-table-resolver
  "Similar to static-table-resolver, but instead of a static map, this will pull the
  table from another attribute in the system. Given in this case the values can be dynamic,
  this helper requires a pre-defined output, so the attributes on this output get
  delegated to the created resolver.

      (def registry
        [(pbir/static-table-resolver `song-names :song/id
           {1 {:song/name \"Marchinha Psicotica de Dr. Soup\"}
            2 {:song/name \"There's Enough\"}})

         (pbir/constantly-resolver ::song-analysis
           {1 {:song/duration 280 :song/tempo 98}
            2 {:song/duration 150 :song/tempo 130}})

         (pbir/attribute-table-resolver ::song-analysis :song/id
           [:song/duration :song/tempo])])

      (let [sm (psm/smart-map (pci/register registry)
                 {:song/id 2})]
        (select-keys sm [:song/id :song/name :song/duration]))
      ; => #:song{:id 2, :name \"There's Enough\", :duration 150}
  "
  [table-name attr-key output]
  [::p.attr/attribute ::p.attr/attribute ::pco/output
   => ::pco/resolver]
  (let [resolver-name (symbol (str (attr-munge attr-key) "-table-" (attr-munge table-name)))]
    (pco/resolver resolver-name
      {::pco/input  [attr-key table-name]
       ::pco/output output}
      (fn [_ input]
        (let [table (get input table-name)
              id    (get input attr-key)]
          (get table id))))))

#?(:clj
   (defn edn-extract-attr-tables
     [data]
     `(into []
            (keep
              (fn [[k# v#]]
                (if-let [attr-key# (and (map? v#)
                                        (:com.wsscode.pathom3/entity-table (meta v#)))]
                  (attribute-table-resolver k# attr-key# (table-output v#)))))
            ~data)))

#?(:clj
   (defmacro edn-file-resolver
     "Creates a resolver to provide data loaded from a file.

     This is a macro and the file will be read at compilation time, this way it
     can work on both Clojure and Clojurescript, without a need for async processing.

     It's also possible to provide static tables data (as with attribute-table-resolver)
     from the data itself, you can do this by setting the meta data :pathom3/entity-table
     meta data in your EDN data. For example:

         {:my.system/generic-db
          ^{:com.wsscode.pathom3/entity-table :my.system/user-id}
          {4 {:my.system.user/name \"Anne\"}
           2 {:my.system.user/name \"Fred\"}}}

     Doing this, the resolvers for the attribute table will be provided automatically.

     Full example:

         ; my-config.edn
         {:my.system/port
          1234

          :my.system/initial-path
          \"/tmp/system\"

          :my.system/generic-db
          ^{:com.wsscode.pathom3/entity-table :my.system/user-id}
          {4 {:my.system.user/name \"Anne\"}
           2 {:my.system.user/name \"Fred\"}}}

         ; app
         (def registry [(edn-file-resolver \"my-config.edn\")])

         (let [sm (psm/smart-map (pci/register registry) {:my.system/user-id 4})]
           (select-keys sm [:my.system/port :my.system.user/name])
         ; => {:my.system/port 1234, :my.system.user/name \"Anne\"}

     Note that the tables need to be a value of a top level property of the config, if
     its deeper inside it won't work."
     [file-path]
     (let [data          (read-string (slurp file-path))
           resolver-name (symbol "edn-file-resolver" (munge file-path))
           output        (pf.eql/data->query data)
           attr-tables   (edn-extract-attr-tables data)]
       `[(pco/resolver '~resolver-name
           {::pco/output ~output}
           (fn ~'[_ _] ~data))
         ~attr-tables])))

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
           (coll/map-keys #(keyword prefix %) (System/getenv)))))))
