(ns com.wsscode.pathom3.trace-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [com.wsscode.misc.time :as time]
    [com.wsscode.pathom3.trace :as p.trace]))

(def example-trace
  '[{::p.trace/span-type
     :com.wsscode.pathom3.interface.eql/process-eql-request,
     ::p.trace/direction
     ::p.trace/direction-enter,
     ::p.trace/span-id   pathom-trace-24742,
     ::p.trace/timestamp 1703036073.860333}
    {::p.trace/span-type
     :com.wsscode.pathom3.interface.eql/process-query->ast,
     ::p.trace/direction
     ::p.trace/direction-enter,
     ::p.trace/span-id        pathom-trace-24743,
     ::p.trace/timestamp      1703036073.87175,
     ::p.trace/parent-span-id pathom-trace-24742}
    {::p.trace/span-type
     :com.wsscode.pathom3.interface.eql/process-query->ast,
     ::p.trace/direction
     ::p.trace/direction-leave,
     ::p.trace/span-id        pathom-trace-24743,
     ::p.trace/timestamp      1703036073.891166,
     ::p.trace/parent-span-id pathom-trace-24742}
    {::p.trace/span-type
     :com.wsscode.pathom3.interface.eql/process-request,
     ::p.trace/direction
     ::p.trace/direction-enter,
     ::p.trace/span-id        pathom-trace-24744,
     ::p.trace/timestamp      1703036073.896958,
     ::p.trace/parent-span-id pathom-trace-24742}
    {::p.trace/span-type
     :com.wsscode.pathom3.connect.runner/process-entity,
     ::p.trace/direction
     ::p.trace/direction-enter,
     ::p.trace/span-id        pathom-trace-24745,
     ::p.trace/timestamp      1703036073.909833,
     ::p.trace/parent-span-id pathom-trace-24744}
    {::p.trace/span-type
     :com.wsscode.pathom3.connect.planner/compute-plan,
     ::p.trace/attributes
     {:com.wsscode.pathom3.connect.planner/cached? false},
     ::p.trace/mode
     ::p.trace/mode-internal,
     ::p.trace/direction
     ::p.trace/direction-enter,
     ::p.trace/span-id        pathom-trace-24746,
     ::p.trace/timestamp      1703036073.970291,
     ::p.trace/parent-span-id pathom-trace-24745}
    {::p.trace/direction
     ::p.trace/direction-leave,
     ::p.trace/span-id        pathom-trace-24746,
     ::p.trace/timestamp      1703036074.183583,
     ::p.trace/parent-span-id pathom-trace-24745}
    {::p.trace/direction
     ::p.trace/direction-leave,
     ::p.trace/span-id        pathom-trace-24745,
     ::p.trace/timestamp      1703036074.348375,
     ::p.trace/parent-span-id pathom-trace-24744}
    {::p.trace/direction
     ::p.trace/direction-leave,
     ::p.trace/span-id        pathom-trace-24744,
     ::p.trace/timestamp      1703036074.399958,
     ::p.trace/parent-span-id pathom-trace-24742}
    {::p.trace/direction
     ::p.trace/direction-leave,
     ::p.trace/span-id   pathom-trace-24742,
     ::p.trace/timestamp 1703036074.402083}])

(deftest trace-test
  (with-redefs [time/now-ms (fn [] 123)
                gensym      (fn [_] 'span-id)]
    (testing "does nothing when there is no trace on env"
      (is (= (p.trace/add-signal! {} {})
             nil)))

    (testing "add a new trace record"
      (let [trace (atom [])]
        (p.trace/add-signal! {::p.trace/trace* trace} {})
        (is (= @trace
               '[{}]))))))

(deftest start-span-test
  (with-redefs [gensym      (fn [_] 'new-sym)
                time/now-ms (fn [] 123)]
    (testing "add a new trace record including direction and returns the span-id"

      (let [trace (atom [])]
        (is (= (p.trace/open-span! {::p.trace/trace* trace} {::p.trace/span-type :test})
               'new-sym))
        (is (= @trace
               '[{::p.trace/attributes  {:com.wsscode.pathom3.path/path []}
                  ::p.trace/signal-type ::p.trace/signal-open-span
                  ::p.trace/span-id     new-sym
                  ::p.trace/span-type   :test
                  ::p.trace/start-time  123}]))))

    (testing "use span-id when given"
      (let [trace (atom [])]
        (p.trace/open-span! {::p.trace/trace* trace} {::p.trace/span-type :test
                                                      ::p.trace/span-id   'my-id})
        (is (= @trace
               '[{::p.trace/attributes  {:com.wsscode.pathom3.path/path []}
                  ::p.trace/signal-type ::p.trace/signal-open-span
                  ::p.trace/span-id     my-id
                  ::p.trace/span-type   :test
                  ::p.trace/start-time  123}]))))

    (testing "use parent-id from env"
      (let [trace (atom [])]
        (p.trace/open-span! {::p.trace/trace* trace ::p.trace/parent-span-id 'parent-span} {::p.trace/span-type :test})
        (is (= @trace
               '[{::p.trace/attributes     {:com.wsscode.pathom3.path/path []}
                  ::p.trace/parent-span-id parent-span
                  ::p.trace/signal-type    ::p.trace/signal-open-span
                  ::p.trace/span-id        new-sym
                  ::p.trace/span-type      :test
                  ::p.trace/start-time     123}]))))

    (testing "prefer parent-id from entity"
      (let [trace (atom [])]
        (p.trace/open-span! {::p.trace/trace* trace ::p.trace/parent-span-id 'parent-span} {::p.trace/span-type      :test
                                                                                            ::p.trace/parent-span-id 'my-parent})
        (is (= @trace
               '[{::p.trace/attributes     {:com.wsscode.pathom3.path/path []}
                  ::p.trace/parent-span-id my-parent
                  ::p.trace/signal-type    ::p.trace/signal-open-span
                  ::p.trace/span-id        new-sym
                  ::p.trace/span-type      :test
                  ::p.trace/start-time     123}]))))))

(deftest finish-span-test
  (testing "add a new trace record to finish the span"
    (with-redefs [time/now-ms (fn [] 123)]
      (let [trace (atom [])]
        (is (= (p.trace/close-span! {::p.trace/trace* trace} 'new-sym)
               'new-sym))
        (is (= @trace
               '[{::p.trace/end-time    123
                  ::p.trace/signal-type ::p.trace/signal-close-span
                  ::p.trace/span-id     new-sym}]))))))

(deftest span-fields-test
  (with-redefs [time/now-ms (fn [] 123)]
    (testing "add a new trace record to finish the span"
      (let [trace (atom [])]
        (p.trace/set-attributes! {::p.trace/trace* trace ::p.trace/parent-span-id 'span-id} {::label "With label"})
        (is (= @trace
               '[{::p.trace/attributes  {:com.wsscode.pathom3.trace-test/label "With label"}
                  ::p.trace/signal-type ::p.trace/signal-attributes
                  ::p.trace/span-id     span-id}])))

      (testing "with explicit span-id"
        (let [trace (atom [])]
          (p.trace/set-attributes! {::p.trace/trace* trace} 'span-id {::label "With label"})
          (is (= @trace
                 '[{::p.trace/attributes  {:com.wsscode.pathom3.trace-test/label "With label"}
                    ::p.trace/signal-type ::p.trace/signal-attributes
                    ::p.trace/span-id     span-id}])))))))

(deftest with-span!-test
  (with-redefs [time/now-ms (fn [] 123)
                gensym      (fn [_] 'span-id)]
    (testing "trace body and support parenting"
      (let [trace (atom [])]
        (p.trace/with-span! [env {::p.trace/env {::p.trace/trace* trace}}]
          (p.trace/with-span! [_ {::p.trace/env env ::p.trace/span-type :c1 ::p.trace/span-id 'c1}] (+ 1 2))
          (p.trace/with-span! [_ {::p.trace/env env ::p.trace/span-type :c2 ::p.trace/span-id 'c2}] (+ 1 2)))
        (is (= @trace
               '[{::p.trace/attributes                  {:com.wsscode.pathom3.path/path []}
                  ::p.trace/signal-type                 ::p.trace/signal-open-span
                  ::p.trace/span-id                     span-id
                  ::p.trace/start-time                  123}
                 {::p.trace/attributes     {:com.wsscode.pathom3.path/path []}
                  ::p.trace/parent-span-id span-id
                  ::p.trace/signal-type    ::p.trace/signal-open-span
                  ::p.trace/span-id        c1
                  ::p.trace/span-type      :c1
                  ::p.trace/start-time     123}
                 {::p.trace/end-time    123
                  ::p.trace/signal-type ::p.trace/signal-close-span
                  ::p.trace/span-id     c1}
                 {::p.trace/attributes     {:com.wsscode.pathom3.path/path []}
                  ::p.trace/parent-span-id span-id
                  ::p.trace/signal-type    ::p.trace/signal-open-span
                  ::p.trace/span-id        c2
                  ::p.trace/span-type      :c2
                  ::p.trace/start-time     123}
                 {::p.trace/end-time    123
                  ::p.trace/signal-type ::p.trace/signal-close-span
                  ::p.trace/span-id     c2}
                 {::p.trace/end-time    123
                  ::p.trace/signal-type ::p.trace/signal-close-span
                  ::p.trace/span-id     span-id}]))))))

(deftest normalize-trace-test
  (testing "basic grouping"
    (is (= (p.trace/normalize-trace
             '[{::p.trace/attributes  {:com.wsscode.pathom3.path/path []}
                ::p.trace/signal-type ::p.trace/signal-open-span
                ::p.trace/span-id     span-id
                ::p.trace/start-time  1}
               {::p.trace/attributes     {:com.wsscode.pathom3.path/path []}
                ::p.trace/parent-span-id span-id
                ::p.trace/signal-type    ::p.trace/signal-open-span
                ::p.trace/span-id        c1
                ::p.trace/span-type      :c1
                ::p.trace/start-time     2}
               {::p.trace/end-time    3
                ::p.trace/signal-type ::p.trace/signal-close-span
                ::p.trace/span-id     c1}
               {::p.trace/attributes     {:com.wsscode.pathom3.path/path []}
                ::p.trace/parent-span-id span-id
                ::p.trace/signal-type    ::p.trace/signal-open-span
                ::p.trace/span-id        c2
                ::p.trace/span-type      :c2
                ::p.trace/start-time     4}
               {::p.trace/end-time    5
                ::p.trace/signal-type ::p.trace/signal-close-span
                ::p.trace/span-id     c2}
               {::p.trace/end-time    6
                ::p.trace/signal-type ::p.trace/signal-close-span
                ::p.trace/span-id     span-id}])
           '{c1      {::p.trace/attributes     {:com.wsscode.pathom3.path/path []}
                      ::p.trace/end-time       3
                      ::p.trace/parent-span-id span-id
                      ::p.trace/span-id        c1
                      ::p.trace/span-type      :c1
                      ::p.trace/start-time     2}
             c2      {::p.trace/attributes     {:com.wsscode.pathom3.path/path []}
                      ::p.trace/end-time       5
                      ::p.trace/parent-span-id span-id
                      ::p.trace/span-id        c2
                      ::p.trace/span-type      :c2
                      ::p.trace/start-time     4}
             nil     {::p.trace/span-children #{span-id}}
             span-id {::p.trace/attributes    {:com.wsscode.pathom3.path/path []}
                      ::p.trace/end-time      6
                      ::p.trace/span-children #{c1
                                                c2}
                      ::p.trace/span-id       span-id
                      ::p.trace/start-time    1}})))

  (testing "overriding fields"
    (is (= (p.trace/normalize-trace
             '[{::p.trace/signal-type ::p.trace/signal-open-span
                ::p.trace/attributes  {:com.wsscode.pathom3.path/path []}
                ::p.trace/span-id     span-id
                ::p.trace/start-time  1}
               {::p.trace/signal-type ::p.trace/signal-attributes
                ::p.trace/span-id     span-id
                ::p.trace/attributes  {:foo "bar"}}
               {::p.trace/end-time    6
                ::p.trace/signal-type ::p.trace/signal-close-span
                ::p.trace/span-id     span-id}])
           '{nil     {::p.trace/span-children #{span-id}}
             span-id {::p.trace/attributes {:com.wsscode.pathom3.path/path []
                                            :foo                           "bar"}
                      ::p.trace/end-time   6
                      ::p.trace/span-id    span-id
                      ::p.trace/start-time 1}}))))

(deftest trace->tree-test
  (with-redefs [time/now-ms (fn [] 123)
                gensym      (fn [_] 'span-id)]
    (testing "blank example"
      (is (= (p.trace/trace->tree [])
             '{::p.trace/span-children ()})))

    (testing "properly group children, making the tree"
      (let [trace (atom [])]
        (p.trace/with-span! [env {::p.trace/env {::p.trace/trace* trace}}]
          (p.trace/with-span! [_ {::p.trace/env env ::p.trace/span-type :c1 ::p.trace/span-id 'c1}] (+ 1 2))
          (p.trace/with-span! [_ {::p.trace/env env ::p.trace/span-type :c2 ::p.trace/span-id 'c2}] (+ 1 2)))
        (is (= (p.trace/trace->tree @trace)
               '{::p.trace/span-children ({::p.trace/attributes    {:com.wsscode.pathom3.path/path []}
                                           ::p.trace/end-time      123
                                           ::p.trace/span-children ({::p.trace/attributes     {:com.wsscode.pathom3.path/path []}
                                                                     ::p.trace/end-time       123
                                                                     ::p.trace/parent-span-id span-id
                                                                     ::p.trace/span-children  ()
                                                                     ::p.trace/span-id        c2
                                                                     ::p.trace/span-type      :c2
                                                                     ::p.trace/start-time     123}
                                                                    {::p.trace/attributes     {:com.wsscode.pathom3.path/path []}
                                                                     ::p.trace/end-time       123
                                                                     ::p.trace/parent-span-id span-id
                                                                     ::p.trace/span-children  ()
                                                                     ::p.trace/span-id        c1
                                                                     ::p.trace/span-type      :c1
                                                                     ::p.trace/start-time     123})
                                           ::p.trace/span-id       span-id
                                           ::p.trace/start-time    123})}))))))
