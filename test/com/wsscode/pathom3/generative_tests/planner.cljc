(ns com.wsscode.pathom3.generative-tests.planner
  (:require
    [clojure.spec.alpha :as s]
    [clojure.spec.test.alpha :as s.test]
    [clojure.test :refer [deftest is are run-tests testing]]
    [clojure.test.check :as tc]
    [clojure.test.check.clojure-test :as test]
    [clojure.test.check.generators :as gen]
    [clojure.test.check.properties :as props]
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.interface.eql :as p.eql]
    [edn-query-language.gen :as eql-gen]))

(defn next-attr [attribute]
  (let [[base current-index]
        (or (some->
              (re-find #"(.+)(\d+)$" (name attribute))
              (->> (drop 1))
              (vec)
              (update 1 #(Long/parseLong %)))
            [(name attribute) 0])]
    (keyword (str base (inc current-index)))))

(def gen-env
  {::p.attr/attribute
   :a

   ::used-attributes
   #{}

   ::max-resolver-depth
   5

   ::gen-root-resolver
   (fn [{::p.attr/keys [attribute]}]
     (gen/return
       {::resolvers [{::pco/op-name (symbol attribute)
                      ::pco/output  [attribute]
                      ::pco/resolve (fn [_ _]
                                      {attribute (name attribute)})}]
        ::query     [attribute]
        ::expected  {attribute (name attribute)}}))

   ::gen-root-blank-resolver
   (fn [{::p.attr/keys [attribute]}]
     (gen/return
       {::resolvers [{::pco/op-name (symbol attribute)
                      ::pco/output  [attribute]
                      ::pco/resolve (fn [_ _]
                                      {})}]
        ::query     [attribute]
        ::expected  {}}))

   ::gen-single-dep-resolver
   (fn [{::p.attr/keys [attribute]
         ::keys        [gen-resolver]
         :as           env}]
     (let [next-attr (next-attr attribute)]
       (gen/let [next-group (gen-resolver
                              (assoc env
                                ::p.attr/attribute next-attr))]
         (gen/return
           {::resolvers (into
                          [{::pco/op-name (symbol attribute)
                            ::pco/input   [next-attr]
                            ::pco/output  [attribute]
                            ::pco/resolve (fn [_ _]
                                            {attribute (name attribute)})}]
                          (::resolvers next-group))
            ::query     [attribute]
            ::expected  {attribute (name attribute)}}))))

   ::gen-resolver
   (fn [{::keys [max-resolver-depth
                 gen-root-resolver
                 gen-root-blank-resolver
                 gen-single-dep-resolver] :as env}]
     (let [env (update env ::max-resolver-depth dec)]
       (if (pos? max-resolver-depth)
         (gen/one-of
           [(gen-root-resolver env)
            ; (gen-root-blank-resolver env)
            (gen-single-dep-resolver env)])
         (gen-root-resolver env))))})

(defn run-generated-test
  [{::keys [resolvers query expected]}]
  (let [env (pci/register (mapv pco/resolver resolvers))]
    (= (p.eql/process env query) expected)))

(comment
  (gen/generate
    ((::gen-resolver gen-env)
     gen-env))

  (run-generated-test
    (gen/generate
      ((::gen-resolver gen-env)
       gen-env)))

  (run-generated-test
    {::resolvers
     [{::pco/op-name 'a
       ::pco/output  [:a]
       ::pco/resolve (fn [_ _] {:a "a"})}]

     ::query
     [:a]

     ::expected
     {:a "a"}})

  (run-generated-test
    {::resolvers
     [{::pco/op-name 'a
       ::pco/output  [:a]
       ::pco/resolve (fn [_ _])}]

     ::query
     [:a]

     ::expected
     {}})

  (run-generated-test
    {::resolvers
     []

     ::query
     [:a]

     ::expected
     {}})

  (let [attribute :a]
    (gen/generate
      (gen/let [resolvers
                (gen/hash-map
                  ::pco/op-name (symbol))]
        )))

  (gen/sample
    (gen/vector-distinct (s/gen #{:a :b :c :d :e})))



  (let [attribute-spectrum]
    (gen/generate
      (gen/let [attribute-spectrum
                (gen/set
                  (gen/elements [:a :b :c :d :e]))

                attribute-resolutions
                (gen/fmap
                  (fn [])
                  attribute-spectrum)

                resolvers
                (gen/hash-map
                  )

                #_#_query
                    (gen/generate
                      (eql-gen/make-gen
                        {::eql-gen/gen-query-expr
                         (fn gen-query-expr [{::eql-gen/keys [gen-property] :as env}]
                           (gen-property env))

                         ::eql-gen/gen-query
                         (fn gen-query [{::eql-gen/keys [gen-property gen-query-expr gen-max-depth] :as env}]
                           (gen/vector-distinct (gen-property env)
                             {:min-elements 0 :max-elements 5}))}

                        ::eql-gen/gen-query))]
        ))))
