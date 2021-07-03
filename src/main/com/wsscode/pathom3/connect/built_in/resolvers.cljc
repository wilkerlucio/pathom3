(ns com.wsscode.pathom3.connect.built-in.resolvers
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.format.eql :as pf.eql])
  #?(:cljs
     (:require-macros
       [com.wsscode.pathom3.connect.built-in.resolvers])))

(>def ::entity-table (s/map-of any? map?))

(defn attr-munge [attr]
  (munge (subs (str attr) 1)))

(defn combine-names [na nb]
  (cond
    (not nb)
    na

    (not na)
    nb

    (= na nb)
    na

    :else
    (str (str na "->" nb))))

(defn attr-alias-resolver-name [from to]
  (symbol
    (combine-names
      (or (namespace from) "-unqualified")
      (or (namespace to) "-unqualified"))
    (combine-names (name from) (name to))))

(defn alias-resolver
  "Create a resolver that will convert attribute `from` to a attribute `to` with
  the same value. This only creates the alias in one direction."
  [from to]
  (let [resolver-name (symbol (str (attr-alias-resolver-name from to) "--alias"))]
    (pco/resolver resolver-name
      {::pco/input  [from]
       ::pco/output [to]
       ::pco/cache? false}
      (fn [_ input] {to (get input from)}))))

(defn equivalence-resolver
  "Make two attributes equivalent. It's like alias-resolver, but returns a vector containing the alias in both directions."
  [attribute-a attribute-b]
  [(alias-resolver attribute-a attribute-b)
   (alias-resolver attribute-b attribute-a)])

(defn constantly-resolver
  "Create a simple resolver that always return `value` for `attribute`."
  ([attribute value]
   (let [resolver-name (symbol (str (attr-munge attribute) "--constant"))]
     (pco/resolver resolver-name
       {::pco/output [attribute]
        ::pco/cache? false}
       (fn [_ _] {attribute value})))))

(defn constantly-fn-resolver
  "Create a simple resolver that always calls value-fn and return its value. Note that
  cache is disabled by default in this resolver."
  ([attribute value-fn]
   (let [resolver-name (symbol (str (attr-munge attribute) "--constant"))]
     (pco/resolver resolver-name
       {::pco/output [attribute]
        ::pco/cache? false}
       (fn [env _] {attribute (value-fn env)})))))

(defn single-attr-resolver
  "Apply fn `f` to input `source` and spits the result with the name `target`.

  `f` receives a single argument, which is the attribute value from `source`."
  [source target f]
  (let [resolver-name (symbol (str (attr-alias-resolver-name source target) "--single-attr-transform"))]
    (pco/resolver resolver-name
      {::pco/input  [source]
       ::pco/output [target]}
      (fn [_ input]
        {target (f (get input source))}))))

(defn single-attr-with-env-resolver
  "Similar single-attr-resolver, but `f` receives two arguments, `env` and the input."
  [source target f]
  (let [resolver-name (symbol (str (attr-alias-resolver-name source target) "--single-attr-transform"))]
    (pco/resolver resolver-name
      {::pco/input  [source]
       ::pco/output [target]}
      (fn [env input]
        {target (f env (get input source))}))))

(defn table-output
  "For a given static table, compute the accumulated output query of the entity values."
  [table]
  (let [[{:keys [output]}] (pf.eql/data->query {:output (vec (vals table))})]
    output))

(>defn static-table-resolver
  "Exposes data for entities, indexes by attr-key. This is a simple way to extend/provide
  data for entities using simple Clojure maps. Example:

      (def registry
        [(pbir/static-table-resolver :song/id
           {1 {:song/name \"Marchinha Psicotica de Dr. Soup\"}
            2 {:song/name \"There's Enough\"}})

         ; you can provide a name for the resolver, if so, prefer fully qualified symbols
         (pbir/static-table-resolver `song-analysis :song/id
           {1 {:song/duration 280 :song/tempo 98}
            2 {:song/duration 150 :song/tempo 130}})])

      (let [sm (psm/smart-map (pci/register registry)
                 {:song/id 1})]
        (select-keys sm [:song/id :song/name :song/duration]))
      ; => #:song{:id 1, :name \"Marchinha Psicotica de Dr. Soup\", :duration 280}

  In this example, we create two different tables that provides data about songs, the
  entities are related by the keys on the table, the `attr-key` says what's the attribute
  name to be used to related the data, in this case we use `:song/id` on both, so they
  get connected by it.
  "
  ([attr-key table]
   [::p.attr/attribute ::entity-table
    => ::pco/resolver]
   (let [resolver-name (symbol (str (attr-alias-resolver-name attr-key "--static-table")))]
     (static-table-resolver resolver-name attr-key table)))
  ([resolver-name attr-key table]
   [::pco/op-name ::p.attr/attribute ::entity-table
    => ::pco/resolver]
   (let [output (table-output table)]
     (pco/resolver resolver-name
       {::pco/input  [attr-key]
        ::pco/output output}
       (fn [_ input]
         (let [id (get input attr-key)]
           (get table id)))))))

(>defn static-attribute-map-resolver
  "This is like the static-table-resolver, but provides a single attribute on each
  map entry.

      (def registry
        [(pbir/attribute-map :song/id :song/name
           {1 \"Marchinha Psicotica de Dr. Soup\"
            2 \"There's Enough\"})

         (pbir/static-table-resolver `song-analysis :song/id
           {1 {:song/duration 280 :song/tempo 98}
            2 {:song/duration 150 :song/tempo 130}})])

      (let [sm (psm/smart-map (pci/register registry)
                 {:song/id 1})]
        (select-keys sm [:song/id :song/name :song/duration]))
      ; => #:song{:id 1, :name \"Marchinha Psicotica de Dr. Soup\", :duration 280}
    "
  [input output mapping]
  [::p.attr/attribute ::p.attr/attribute map?
   => ::pco/resolver]
  (let [resolver-name (symbol (str (attr-alias-resolver-name input output) "--static-attribute-map"))]
    (pco/resolver resolver-name
      {::pco/input  [input]
       ::pco/output [output]}
      (fn [_ input-map]
        (if-let [x (find mapping (get input-map input))]
          {output (val x)})))))

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
  (let [resolver-name (symbol (str (attr-munge attr-key) "--table-" (attr-munge table-name)))]
    (pco/resolver resolver-name
      {::pco/input  [attr-key table-name]
       ::pco/output output}
      (fn [_ input]
        (let [table (get input table-name)
              id    (get input attr-key)]
          (get table id))))))

(>defn env-table-resolver
  "Similar to attribute-table-resolver, but pulls table from env instead of other resolver.

      (def registry
        [(pbir/static-table-resolver `song-names :song/id
           {1 {:song/name \"Marchinha Psicotica de Dr. Soup\"}
            2 {:song/name \"There's Enough\"}})

         (pbir/env-table-resolver ::song-analysis :song/id
           [:song/duration :song/tempo])])

      (def table
        {::song-analysis
          {1 {:song/duration 280 :song/tempo 98}
           2 {:song/duration 150 :song/tempo 130}}})

      ;                                          merge table into env
      (let [sm (psm/smart-map (merge (pci/register registry) table)
                 {:song/id 2})]
        (select-keys sm [:song/id :song/name :song/duration]))
      ; => #:song{:id 2, :name \"There's Enough\", :duration 150}
  "
  [table-name attr-key output]
  [::p.attr/attribute ::p.attr/attribute ::pco/output
   => ::pco/resolver]
  (let [resolver-name (symbol (str (attr-munge attr-key) "--env-table-" (attr-munge table-name)))]
    (pco/resolver resolver-name
      {::pco/input  [attr-key]
       ::pco/output output}
      (fn [env input]
        (let [table (get env table-name)
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

     Note that the tables need to be a value of a top level attribute of the config, if
     its deeper inside it won't work."
     [file-path]
     (let [data          (read-string (slurp file-path))
           resolver-name (symbol "com.wsscode.pathom3.edn-file-resolver" (munge file-path))
           output        (pf.eql/data->query data)
           attr-tables   (edn-extract-attr-tables data)]
       `[(pco/resolver '~resolver-name
           {::pco/output ~output}
           (fn ~'[_ _] ~data))
         ~attr-tables])))

(defn extract-attr-tables
  [data]
  (into []
        (keep
          (fn [[k v]]
            (if-let [attr-key (and (map? v)
                                   (:com.wsscode.pathom3/entity-table (meta v)))]
              (attribute-table-resolver k attr-key (table-output v)))))
        data))

(defn global-data-resolver
  "Expose data as a resolver, note this data will be available everywhere in the system.

  Works the same as edn-file-resolver, but uses the data directly instead of reading
  from a file. This also applies for the attribute tables inside the data."
  [data]
  (let [resolver-name (symbol "com.wsscode.pathom3.global-data-resolver" (str (hash data)))
        output        (pf.eql/data->query data)
        attr-tables   (extract-attr-tables data)]
    [(pco/resolver resolver-name
       {::pco/output output}
       (fn global-data-resolver-fn [_ _] data))
     attr-tables]))
