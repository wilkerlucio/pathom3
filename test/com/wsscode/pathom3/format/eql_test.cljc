(ns com.wsscode.pathom3.format.eql-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.pathom3.format.eql :as pf.eql]
    [com.wsscode.pathom3.plugin :as p.plugin]
    [com.wsscode.pathom3.test.helpers :as th]
    [edn-query-language.core :as eql]))

(defn with-rs [x]
  (with-meta x {:com.wsscode.pathom3.connect.runner/run-stats {}}))

(deftest query-root-properties-test
  (is (= (pf.eql/query-root-properties [{:a [:b]} :c])
         [:a :c]))

  (is (= (pf.eql/query-root-properties {:foo [{:a [:b]} :c]
                                        :bar [:a :d]})
         [:a :c :d])))

(deftest union-children?-test
  (is (true? (pf.eql/union-children? (eql/query->ast1 [{:union {:a [:foo]}}]))))
  (is (false? (pf.eql/union-children? (eql/query->ast1 [:standard])))))

(deftest maybe-merge-union-ast-test
  (is (= (-> [{:union {:a [:foo]
                       :b [:bar]}}]
             (eql/query->ast1)
             (pf.eql/maybe-merge-union-ast)
             (eql/ast->query))
         [{:union [:foo :bar]}]))

  (is (= (-> [{:not-union [:baz]}]
             (eql/query->ast1)
             (pf.eql/maybe-merge-union-ast)
             (eql/ast->query))
         [{:not-union [:baz]}])))

(deftest ident-key-test
  (is (= (pf.eql/ident-key [:foo "bar"])
         :foo)))

(deftest index-ast-test
  (is (= (pf.eql/index-ast (eql/query->ast [:foo {:bar [:baz]}]))
         {:foo {:type :prop, :dispatch-key :foo, :key :foo},
          :bar {:type         :join,
                :dispatch-key :bar,
                :key          :bar,
                :query        [:baz],
                :children     [{:type :prop, :dispatch-key :baz, :key :baz}]}}))

  (testing "repetitions go away"
    (is (= (pf.eql/index-ast (eql/query->ast [:a :b :a :a :c :b]))
           {:a {:type :prop, :dispatch-key :a, :key :a},
            :b {:type :prop, :dispatch-key :b, :key :b},
            :c {:type :prop, :dispatch-key :c, :key :c}})))

  (testing "placeholders"
    (is (= (pf.eql/index-ast (eql/query->ast [:foo
                                              {:>/ph [:bar]}]))
           {:>/ph {:children     [{:dispatch-key :bar
                                   :key          :bar
                                   :type         :prop}]
                   :dispatch-key :>/ph
                   :key          :>/ph
                   :query        [:bar]
                   :type         :join}
            :bar  {:dispatch-key :bar
                   :key          :bar
                   :type         :prop}
            :foo  {:dispatch-key :foo
                   :key          :foo
                   :type         :prop}}))

    (testing "merged"
      (is (= (pf.eql/index-ast (eql/query->ast [:foo
                                                {:>/ph [:bar]}
                                                {:>/ph [:baz]}]))
             {:foo {:type :prop, :dispatch-key :foo, :key :foo},
              :bar {:type :prop, :dispatch-key :bar, :key :bar},
              :>/ph {:type :join,
                     :dispatch-key :>/ph,
                     :key :>/ph,
                     :children [{:type :prop, :dispatch-key :bar, :key :bar}
                                {:type :prop, :dispatch-key :baz, :key :baz}]},
              :baz {:type :prop, :dispatch-key :baz, :key :baz}})))

    (testing "nested"
      (is (= (pf.eql/index-ast (eql/query->ast [:foo
                                                {:>/ph [:bar
                                                        {:>/more [:baz]}]}]))
             {:>/ph {:children     [{:dispatch-key :bar
                                     :key          :bar
                                     :type         :prop}
                                    {:children     [{:dispatch-key :baz
                                                     :key          :baz
                                                     :type         :prop}]
                                     :dispatch-key :>/more
                                     :key          :>/more
                                     :query        [:baz]
                                     :type         :join}]
                     :dispatch-key :>/ph
                     :key          :>/ph
                     :query        [:bar
                                    {:>/more [:baz]}]
                     :type         :join}
              :bar  {:dispatch-key :bar
                     :key          :bar
                     :type         :prop}
              :baz  {:dispatch-key :baz
                     :key          :baz
                     :type         :prop}
              :foo  {:dispatch-key :foo
                     :key          :foo
                     :type         :prop}}))))

  (testing "errors on different params"
    (is (thrown-with-msg?
          #?(:clj Throwable :cljs :default)
          #"Can't merge different params"
          (pf.eql/index-ast (eql/query->ast '[(:a {:x 1}) (:a {:y 1})])))))

  (testing "merging sub queries"
    (is (= (pf.eql/index-ast (eql/query->ast [{:bar [:baz]} {:bar [:bur]}]))
           {:bar {:type         :join,
                  :dispatch-key :bar,
                  :key          :bar,
                  :children     [{:type :prop, :dispatch-key :baz, :key :baz}
                                 {:type :prop, :dispatch-key :bur, :key :bur}]}}))

    (testing "under placeholder"
      (is (= (pf.eql/index-ast (eql/query->ast [{:bar [:baz]}
                                                {:>/ph [{:bar [:bur]}]}]))
             {:>/ph {:children     [{:children     [{:dispatch-key :bur
                                                     :key          :bur
                                                     :type         :prop}]
                                     :dispatch-key :bar
                                     :key          :bar
                                     :query        [:bur]
                                     :type         :join}]
                     :dispatch-key :>/ph
                     :key          :>/ph
                     :query        [{:bar [:bur]}]
                     :type         :join}
              :bar  {:children     [{:dispatch-key :baz
                                     :key          :baz
                                     :type         :prop}
                                    {:dispatch-key :bur
                                     :key          :bur
                                     :type         :prop}]
                     :dispatch-key :bar
                     :key          :bar
                     :type         :join}}))))

  (testing "remove *"
    (is (= (pf.eql/index-ast (eql/query->ast [:foo '*]))
           {:foo {:type :prop, :dispatch-key :foo, :key :foo},})))

  (testing "::wrap-index-ast-entry"
    (testing "can ignore placeholder parts"
      (is (= (pf.eql/index-ast
               (p.plugin/register
                 {::p.plugin/id 'cancel
                  ::pf.eql/wrap-index-ast-entry
                  (fn [wrap-entry]
                    (fn [env m node]
                      (if (some-> node :params :skip?)
                        m
                        (wrap-entry env m node))))})
               (eql/query->ast [:foo
                                {'(:>/ph {:skip? true}) [:bar]}
                                {:>/ph2 [:baz]}]))
             {:>/ph2 {:children     [{:dispatch-key :baz
                                      :key          :baz
                                      :type         :prop}]
                      :dispatch-key :>/ph2
                      :key          :>/ph2
                      :query        [:baz]
                      :type         :join}
              :baz   {:dispatch-key :baz
                      :key          :baz
                      :type         :prop}
              :foo   {:dispatch-key :foo
                      :key          :foo
                      :type         :prop}})))))

(def protected-list #{:foo})

(p.plugin/defplugin elide-specials
  {::pf.eql/wrap-map-select-entry
   (fn [mst]
     (fn [env source {:keys [key] :as ast}]
       (if (and (contains? source key)
                (contains? protected-list key))
         (coll/make-map-entry key "Protected value")
         (mst env source ast))))})

(defrecord RecordSample [foo])

(deftest map-select-test
  (is (= (pf.eql/map-select {} {} [:foo :bar])
         {}))

  (is (= (pf.eql/map-select {} {:foo 123} [:foo :bar])
         {:foo 123}))

  (is (= (pf.eql/map-select {} {:foo {:a 1 :b 2}} [{:foo [:b]}])
         {:foo {:b 2}}))

  (testing "process vector"
    (is (= (pf.eql/map-select {} {:foo [{:a 1 :b 2}
                                        {:c 1 :b 1}
                                        {:a 1 :c 1}
                                        3]}
                              [{:foo [:b]}])
           {:foo [{:b 2} {:b 1} {} 3]})))

  (testing "recursive query"
    (is (= (pf.eql/map-select {}
                              {:x "a"
                               :y "aa"
                               :c [{:x   "b"
                                    :bla "bb"
                                    :c   [{:x        "c"
                                           :whatever "d"}]}]}
                              [:x {:c '...}])
           {:x "a"
            :c [{:x "b"
                 :c [{:x "c"}]}]})))

  (testing "retain set type"
    (is (= (pf.eql/map-select {} {:foo #{{:a 1 :b 2}
                                         {:c 1 :b 1}}}
                              [{:foo [:b]}])
           {:foo #{{:b 2} {:b 1}}})))

  (testing "retain list order"
    (is (= (pf.eql/map-select {} {:foo (list 1 2)}
                              [:foo])
           {:foo (list 1 2)})))

  (testing "union"
    (is (= (pf.eql/map-select {} {:foo [{:a 1 :aa 2 :aaa 3}
                                        {:b 2 :bb 10 :bbb 20}
                                        {:c 3 :cc 30 :ccc 300}]}
                              [{:foo {:a [:aa]
                                      :b [:b]
                                      :c [:ccc]}}])
           {:foo [{:aa 2} {:b 2} {:ccc 300}]})))

  (testing "*"
    (is (= (pf.eql/map-select {} {:foo 1 :bar 2} [:foo '*])
           {:foo 1 :bar 2})))

  (testing "extended"
    (is (= (pf.eql/map-select (p.plugin/register elide-specials) {:foo 1 :bar 2} [:foo :bar])
           {:foo "Protected value" :bar 2}))

    (is (= (pf.eql/map-select (p.plugin/register elide-specials) {:deep {:foo "bar"}}
                              ['*])
           {:deep {:foo "Protected value"}}))

    (is (= (pf.eql/map-select (p.plugin/register elide-specials) {:deep {:foo "bar"}}
                              [:deep])
           {:deep {:foo "Protected value"}})))

  (testing "custom records"
    (let [record (->RecordSample "bar")]
      (is (= (pf.eql/map-select {} {:foo record} [:foo])
             {:foo record}))))

  (testing "special case: mutations with error"
    (is (= (pf.eql/map-select {}
                              {'foo {:com.wsscode.pathom3.connect.runner/mutation-error "x"}}
                              '[{(foo) [:bar]}])
           {'foo {:com.wsscode.pathom3.connect.runner/mutation-error "x"}})))

  (testing "transient stats"
    (is (= (th/expose-meta
             (pf.eql/map-select {}
                                (with-rs {:bar 10
                                          :foo (with-rs {:a 1})})
                                [:bar]))
           {:bar                                   10,
            :com.wsscode.pathom3.test.helpers/meta {:com.wsscode.pathom3.connect.runner/run-stats {:com.wsscode.pathom3.connect.runner/transient-stats {:foo {}}}}}))

    (testing "placeholders don't get transient meta"
      (is (= (th/expose-meta
               (pf.eql/map-select {}
                                  (with-rs {:bar  10
                                            :foo  (with-rs {:a 1})
                                            :>/ph (with-rs {:bar 10
                                                            :foo (with-rs {:a 1})})})
                                  [:bar
                                   {:>/ph [:bar]}]))
             {:bar                                   10,
              :>/ph                                  {:bar                                   10,
                                                      :com.wsscode.pathom3.test.helpers/meta {:com.wsscode.pathom3.connect.runner/run-stats {}}},
              :com.wsscode.pathom3.test.helpers/meta {:com.wsscode.pathom3.connect.runner/run-stats {:com.wsscode.pathom3.connect.runner/transient-stats {:foo {}}}}})))))

(deftest data->query-test
  (is (= (pf.eql/data->query {}) []))
  (is (= (pf.eql/data->query {:foo "bar"}) [:foo]))
  (is (= (pf.eql/data->query {:b 2 :a 1}) [:a :b]))
  (is (= (pf.eql/data->query {:foo {:buz "bar"}}) [{:foo [:buz]}]))
  (is (= (pf.eql/data->query {:foo [{:buz "bar"}]}) [{:foo [:buz]}]))
  (is (= (pf.eql/data->query {:other "key" [:complex "key"] "value"}) [:other [:complex "key"]]))
  (is (= (pf.eql/data->query {:foo ["abc"]}) [:foo]))
  (is (= (pf.eql/data->query {:foo [{:buz "baz"} {:it "nih"}]}) [{:foo [:buz :it]}]))
  (is (= (pf.eql/data->query {:foo [{:buz "baz"} "abc" {:it "nih"}]}) [{:foo [:buz :it]}]))
  (is (= (pf.eql/data->query {:z 10 :a 1 :b {:d 3 :e 4}}) [:a {:b [:d :e]} :z]))
  (is (= (pf.eql/data->query {:a {"foo" {:bar "baz"}}}) [:a])))

(deftest seq-data->query-test
  (is (= (pf.eql/seq-data->query [{:a 1} {:b 2}]) [:a :b])))

(deftest ast-contains-wildcard?-test
  (is (false? (pf.eql/ast-contains-wildcard? (eql/query->ast [:foo]))))
  (is (true? (pf.eql/ast-contains-wildcard? (eql/query->ast [:foo '*])))))

(deftest pick-union-entry-test
  (is (= (pf.eql/pick-union-entry (eql/query->ast1 [{:foo {:a [:x] :b [:y]}}])
                                  {:b 1})
         {:type :root, :union-key :b
          :children [{:type :prop, :dispatch-key :y, :key :y}]}))

  (testing "via meta on data, which has higher priority than the data"
    (is (= (pf.eql/pick-union-entry (eql/query->ast1 [{:foo {:a [:x] :b [:y]}}])
                                    ^{::pf.eql/union-entry-key :a} {:b 1})
           {:type :root, :union-key :a
            :children [{:type :prop, :dispatch-key :x, :key :x}]}))))

(deftest merge-ast-children-test
  (is (= (pf.eql/merge-ast-children
           (eql/query->ast [])
           (eql/query->ast []))
         (eql/query->ast [])))

  (is (= (pf.eql/merge-ast-children
           (eql/query->ast [:a])
           (eql/query->ast [:b]))
         (eql/query->ast [:a :b])))

  (is (= (pf.eql/merge-ast-children
           nil
           (eql/query->ast [:b]))
         {:type :join, :children [{:type :prop, :dispatch-key :b, :key :b}]}))

  (is (= (pf.eql/merge-ast-children
           (eql/query->ast [:a])
           (eql/query->ast [(list :a {:foo "bar"})]))
         (eql/query->ast [:a])))

  (is (= (pf.eql/merge-ast-children
           nil
           (eql/query->ast1 [:b]))
         (eql/query->ast1 [:b])))

  (is (= (pf.eql/merge-ast-children
           (eql/query->ast [:a])
           (eql/query->ast [:a]))
         {:type     :root,
          :children [{:type :prop, :dispatch-key :a, :key :a}]}))

  (is (= (pf.eql/merge-ast-children
           (eql/query->ast1 [:a])
           (eql/query->ast1 [:a]))
         {:type :prop, :dispatch-key :a, :key :a}))

  (is (= (pf.eql/merge-ast-children
           (eql/query->ast1 [:a])
           (eql/query->ast1 [{:a [:b]}]))
         {:type :join,
          :dispatch-key :a,
          :key :a,
          :children [{:type :prop, :dispatch-key :b, :key :b}]}))

  (is (= (pf.eql/merge-ast-children
           (eql/query->ast [{:a [:b]}])
           (eql/query->ast [:a]))
         {:type     :root,
          :children [{:type         :join,
                      :dispatch-key :a,
                      :key          :a,
                      :children     [{:type :prop, :dispatch-key :b, :key :b}]}]}))

  (is (= (pf.eql/merge-ast-children
           (eql/query->ast [{:a [:b]}])
           (eql/query->ast [{:a [{:b [:c]}]}]))
         {:type     :root,
          :children [{:type         :join,
                      :dispatch-key :a,
                      :key          :a,
                      :children     [{:type         :join,
                                      :dispatch-key :b,
                                      :key          :b,
                                      :children     [{:type :prop, :dispatch-key :c, :key :c}]}]}]}))

  (is (= (pf.eql/merge-ast-children
           (eql/query->ast [{:a [:b]}])
           (eql/query->ast [{:a [{:c [:d]}]}]))
         {:type     :root,
          :children [{:type         :join,
                      :dispatch-key :a,
                      :key          :a,
                      :children     [{:type :prop, :dispatch-key :b, :key :b}
                                     {:type         :join,
                                      :dispatch-key :c,
                                      :key          :c,
                                      :children     [{:type :prop, :dispatch-key :d, :key :d}]}]}]})))

(deftest stats-value?-test
  (is (= (pf.eql/stats-value? {})
         false))
  (is (= (pf.eql/stats-value? (with-rs {}))
         true))
  (is (= (pf.eql/stats-value? [(with-rs {})])
         true))
  (is (= (pf.eql/stats-value? [{}])
         false))
  (is (= (pf.eql/stats-value? 3)
         false))
  (is (= (pf.eql/stats-value? true)
         false))
  (is (= (pf.eql/stats-value? "foo")
         false)))

(deftest select-stats-data-test
  (is (= (pf.eql/select-stats-data
           {:foo   "bar"
            :bar   {}
            :other (with-rs {:a 1
                             :b (with-rs {:d 1})})
            :more  [(with-rs {:b 1})
                    (with-rs {:b 2})
                    (with-rs {:b 3})]})
         {:other {:b {}}, :more [{} {} {}]})))
