(ns com.wsscode.pathom3.trace-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [com.wsscode.misc.time :as time]
    [com.wsscode.pathom3.trace :as p.trace]))

(def example-trace
  '[{::p.trace/event-type
     :com.wsscode.pathom3.interface.eql/process-eql-request,
     ::p.trace/direction
     ::p.trace/direction-enter,
     ::p.trace/span-id   pathom-trace-24742,
     ::p.trace/timestamp 1703036073.860333}
    {::p.trace/event-type
     :com.wsscode.pathom3.interface.eql/process-query->ast,
     ::p.trace/direction
     ::p.trace/direction-enter,
     ::p.trace/span-id   pathom-trace-24743,
     ::p.trace/timestamp 1703036073.87175,
     ::p.trace/parent-id pathom-trace-24742}
    {::p.trace/event-type
     :com.wsscode.pathom3.interface.eql/process-query->ast,
     ::p.trace/direction
     ::p.trace/direction-leave,
     ::p.trace/span-id   pathom-trace-24743,
     ::p.trace/timestamp 1703036073.891166,
     ::p.trace/parent-id pathom-trace-24742}
    {::p.trace/event-type
     :com.wsscode.pathom3.interface.eql/process-request,
     ::p.trace/direction
     ::p.trace/direction-enter,
     ::p.trace/span-id   pathom-trace-24744,
     ::p.trace/timestamp 1703036073.896958,
     ::p.trace/parent-id pathom-trace-24742}
    {::p.trace/event-type
     :com.wsscode.pathom3.connect.runner/process-entity,
     ::p.trace/direction
     ::p.trace/direction-enter,
     ::p.trace/span-id   pathom-trace-24745,
     ::p.trace/timestamp 1703036073.909833,
     ::p.trace/parent-id pathom-trace-24744}
    {::p.trace/event-type
     :com.wsscode.pathom3.connect.planner/compute-plan,
     ::p.trace/fields
     {:com.wsscode.pathom3.connect.planner/cached? false},
     ::p.trace/mode
     ::p.trace/mode-internal,
     ::p.trace/direction
     ::p.trace/direction-enter,
     ::p.trace/span-id   pathom-trace-24746,
     ::p.trace/timestamp 1703036073.970291,
     ::p.trace/parent-id pathom-trace-24745}
    {::p.trace/direction
     ::p.trace/direction-leave,
     ::p.trace/span-id   pathom-trace-24746,
     ::p.trace/timestamp 1703036074.183583,
     ::p.trace/parent-id pathom-trace-24745}
    {::p.trace/direction
     ::p.trace/direction-leave,
     ::p.trace/span-id   pathom-trace-24745,
     ::p.trace/timestamp 1703036074.348375,
     ::p.trace/parent-id pathom-trace-24744}
    {::p.trace/direction
     ::p.trace/direction-leave,
     ::p.trace/span-id   pathom-trace-24744,
     ::p.trace/timestamp 1703036074.399958,
     ::p.trace/parent-id pathom-trace-24742}
    {::p.trace/direction
     ::p.trace/direction-leave,
     ::p.trace/span-id   pathom-trace-24742,
     ::p.trace/timestamp 1703036074.402083}])

(deftest trace-test
  (with-redefs [time/now-ms (fn [] 123)
                gensym      (fn [_] 'span-id)]
    (testing "does nothing when there is no trace on env"
      (is (= (p.trace/trace {} {::p.trace/event-type :test})
             nil)))

    (testing "add a new trace record"
      (let [trace (atom [])]
        (p.trace/trace {::p.trace/trace* trace} {::p.trace/event-type :test})
        (is (= @trace
               '[{::p.trace/event-type :test
                  ::p.trace/span-id    span-id
                  ::p.trace/timestamp  123}]))))

    (testing "use span-id when given"
      (let [trace (atom [])]
        (p.trace/trace {::p.trace/trace* trace} {::p.trace/event-type :test
                                                 ::p.trace/span-id    'my-id})
        (is (= @trace
               '[{::p.trace/event-type :test
                  ::p.trace/span-id    my-id
                  ::p.trace/timestamp  123}]))))

    (testing "use parent-id from env"
      (let [trace (atom [])]
        (p.trace/trace {::p.trace/trace* trace ::p.trace/parent-id 'parent-span} {::p.trace/event-type :test})
        (is (= @trace
               '[{::p.trace/event-type :test
                  ::p.trace/span-id    span-id
                  ::p.trace/parent-id  parent-span
                  ::p.trace/timestamp  123}]))))

    (testing "prefer parent-id from entity"
      (let [trace (atom [])]
        (p.trace/trace {::p.trace/trace* trace ::p.trace/parent-id 'parent-span} {::p.trace/event-type :test
                                                                                  ::p.trace/parent-id  'my-parent})
        (is (= @trace
               '[{::p.trace/event-type :test
                  ::p.trace/span-id    span-id
                  ::p.trace/parent-id  my-parent
                  ::p.trace/timestamp  123}]))))))

(deftest start-span-test
  (testing "add a new trace record including direction and returns the span-id"
    (with-redefs [gensym      (fn [_] 'new-sym)
                  time/now-ms (fn [] 123)]
      (let [trace (atom [])]
        (is (= (p.trace/start-span {::p.trace/trace* trace} {::p.trace/event-type :test})
               'new-sym))
        (is (= @trace
               '[{::p.trace/event-type :test
                  ::p.trace/direction  ::p.trace/direction-enter
                  ::p.trace/span-id    new-sym
                  ::p.trace/timestamp  123}]))))))

(deftest finish-span-test
  (testing "add a new trace record to finish the span"
    (with-redefs [time/now-ms (fn [] 123)]
      (let [trace (atom [])]
        (is (= (p.trace/finish-span {::p.trace/trace* trace} 'new-sym)
               'new-sym))
        (is (= @trace
               '[{::p.trace/direction ::p.trace/direction-leave
                  ::p.trace/span-id   new-sym
                  ::p.trace/timestamp 123}]))))))

(deftest span-fields-test
  (with-redefs [time/now-ms (fn [] 123)]
    (testing "add a new trace record to finish the span"
      (let [trace (atom [])]
        (p.trace/span-fields {::p.trace/trace* trace ::p.trace/parent-id 'span-id} {::label "With label"})
        (is (= @trace
               '[{::p.trace/span-id   span-id
                  ::p.trace/fields    {::label "With label"}
                  ::p.trace/timestamp 123}])))

      (testing "with explicit span-id"
        (let [trace (atom [])]
          (p.trace/span-fields {::p.trace/trace* trace} 'span-id {::label "With label"})
          (is (= @trace
                 '[{::p.trace/span-id   span-id
                    ::p.trace/fields    {::label "With label"}
                    ::p.trace/timestamp 123}])))))))

(deftest tracing-test
  (with-redefs [time/now-ms (fn [] 123)
                gensym      (fn [_] 'span-id)]
    (testing "trace the body"
      (let [trace (atom [])]
        (p.trace/tracing {::p.trace/trace* trace} {::label "With label"}
                         (+ 1 2))
        (is (= @trace
               '[{:com.wsscode.pathom3.trace-test/label "With label"
                  ::p.trace/direction                   ::p.trace/direction-enter
                  ::p.trace/span-id                     span-id
                  ::p.trace/timestamp                   123}
                 {::p.trace/direction ::p.trace/direction-leave
                  ::p.trace/span-id   span-id
                  ::p.trace/timestamp 123}]))))))

(deftest tracing-with-parent-test
  (with-redefs [time/now-ms (fn [] 123)
                gensym      (fn [_] 'span-id)]
    (testing "trace body and support parenting"
      (let [trace (atom [])]
        (p.trace/tracing-with-parent {::p.trace/trace* trace} {::label "With label"}
                                     (fn [env]
                                       (p.trace/tracing env {::p.trace/event-type :c1 ::p.trace/span-id 'c1} (+ 1 2))
                                       (p.trace/tracing env {::p.trace/event-type :c2 ::p.trace/span-id 'c2} (+ 1 2))))
        (is (= @trace
               '[{:com.wsscode.pathom3.trace-test/label "With label"
                  ::p.trace/direction                   ::p.trace/direction-enter
                  ::p.trace/span-id                     span-id
                  ::p.trace/timestamp                   123}
                 {::p.trace/direction  ::p.trace/direction-enter
                  ::p.trace/event-type :c1
                  ::p.trace/parent-id  span-id
                  ::p.trace/span-id    c1
                  ::p.trace/timestamp  123}
                 {::p.trace/direction ::p.trace/direction-leave
                  ::p.trace/parent-id span-id
                  ::p.trace/span-id   c1
                  ::p.trace/timestamp 123}
                 {::p.trace/direction  ::p.trace/direction-enter
                  ::p.trace/event-type :c2
                  ::p.trace/parent-id  span-id
                  ::p.trace/span-id    c2
                  ::p.trace/timestamp  123}
                 {::p.trace/direction ::p.trace/direction-leave
                  ::p.trace/parent-id span-id
                  ::p.trace/span-id   c2
                  ::p.trace/timestamp 123}
                 {::p.trace/direction ::p.trace/direction-leave
                  ::p.trace/span-id   span-id
                  ::p.trace/timestamp 123}]))))))

(deftest normalize-trace-test
  (testing "basic grouping"
    (is (= (p.trace/normalize-trace
             '[{:com.wsscode.pathom3.trace-test/label "With label"
                ::p.trace/direction                   ::p.trace/direction-enter
                ::p.trace/span-id                     span-id
                ::p.trace/timestamp                   1}
               {::p.trace/direction  ::p.trace/direction-enter
                ::p.trace/event-type :c1
                ::p.trace/parent-id  span-id
                ::p.trace/span-id    c1
                ::p.trace/timestamp  2}
               {::p.trace/direction ::p.trace/direction-leave
                ::p.trace/parent-id span-id
                ::p.trace/span-id   c1
                ::p.trace/timestamp 3}
               {::p.trace/direction  ::p.trace/direction-enter
                ::p.trace/event-type :c2
                ::p.trace/parent-id  span-id
                ::p.trace/span-id    c2
                ::p.trace/timestamp  4}
               {::p.trace/direction ::p.trace/direction-leave
                ::p.trace/parent-id span-id
                ::p.trace/span-id   c2
                ::p.trace/timestamp 5}
               {::p.trace/direction ::p.trace/direction-leave
                ::p.trace/span-id   span-id
                ::p.trace/timestamp 6}])
           '{c1      {::p.trace/direction  ::p.trace/direction-enter
                      ::p.trace/duration   1
                      ::p.trace/event-type :c1
                      ::p.trace/parent-id  span-id
                      ::p.trace/span-id    c1
                      ::p.trace/timestamp  2}
             c2      {::p.trace/direction  ::p.trace/direction-enter
                      ::p.trace/duration   1
                      ::p.trace/event-type :c2
                      ::p.trace/parent-id  span-id
                      ::p.trace/span-id    c2
                      ::p.trace/timestamp  4}
             nil     {::p.trace/span-children #{span-id}}
             span-id {:com.wsscode.pathom3.trace-test/label "With label"
                      ::p.trace/direction                   ::p.trace/direction-enter
                      ::p.trace/duration                    5
                      ::p.trace/span-children               #{c1 c2}
                      ::p.trace/span-id                     span-id
                      ::p.trace/timestamp                   1}})))

  (testing "overriding fields"
    (is (= (p.trace/normalize-trace
             '[{:com.wsscode.pathom3.trace-test/label "With label"
                ::p.trace/direction                   ::p.trace/direction-enter
                ::p.trace/span-id                     span-id
                ::p.trace/timestamp                   1}
               {::p.trace/direction  ::p.trace/direction-enter
                ::p.trace/event-type :c1
                ::p.trace/parent-id  span-id
                ::p.trace/span-id    c1
                ::p.trace/timestamp  2}
               {::p.trace/direction ::p.trace/direction-leave
                ::p.trace/parent-id span-id
                ::p.trace/span-id   c1
                ::p.trace/timestamp 3}
               {::p.trace/span-id c1
                ::p.trace/fields  {:foo "bar"}}
               {::p.trace/direction  ::p.trace/direction-enter
                ::p.trace/event-type :c2
                ::p.trace/parent-id  span-id
                ::p.trace/span-id    c2
                ::p.trace/timestamp  4}
               {::p.trace/direction ::p.trace/direction-leave
                ::p.trace/parent-id span-id
                ::p.trace/span-id   c2
                ::p.trace/timestamp 5}
               {::p.trace/direction ::p.trace/direction-leave
                ::p.trace/span-id   span-id
                ::p.trace/timestamp 6}])
           '{c1      {::p.trace/direction  ::p.trace/direction-enter
                      ::p.trace/duration   1
                      ::p.trace/event-type :c1
                      ::p.trace/parent-id  span-id
                      ::p.trace/span-id    c1
                      ::p.trace/fields     {:foo "bar"}
                      ::p.trace/timestamp  2}
             c2      {::p.trace/direction  ::p.trace/direction-enter
                      ::p.trace/duration   1
                      ::p.trace/event-type :c2
                      ::p.trace/parent-id  span-id
                      ::p.trace/span-id    c2
                      ::p.trace/timestamp  4}
             nil     {::p.trace/span-children #{span-id}}
             span-id {:com.wsscode.pathom3.trace-test/label "With label"
                      ::p.trace/direction                   ::p.trace/direction-enter
                      ::p.trace/duration                    5
                      ::p.trace/span-children               #{c1 c2}
                      ::p.trace/span-id                     span-id
                      ::p.trace/timestamp                   1}}))))

(deftest trace->tree-test
  (with-redefs [time/now-ms (fn [] 123)
                gensym      (fn [_] 'span-id)]
    (testing "blank example"
      (is (= (p.trace/trace->tree [])
             '{:com.wsscode.pathom3.trace/span-children ()})))

    (testing "properly group children, making the tree"
      (let [trace (atom [])]
        (p.trace/tracing-with-parent {::p.trace/trace* trace} {::label "With label"}
                                     (fn [env]
                                       (p.trace/tracing env {::p.trace/event-type :c1 ::p.trace/span-id 'c1} (+ 1 2))
                                       (p.trace/tracing env {::p.trace/event-type :c2 ::p.trace/span-id 'c2} (+ 1 2))))
        (is (= (p.trace/trace->tree @trace)
               '{:com.wsscode.pathom3.trace/span-children
                 ({:com.wsscode.pathom3.trace-test/label    "With label"
                   :com.wsscode.pathom3.trace/direction     :com.wsscode.pathom3.trace/direction-enter
                   :com.wsscode.pathom3.trace/duration      0
                   :com.wsscode.pathom3.trace/span-children ({:com.wsscode.pathom3.trace/direction     :com.wsscode.pathom3.trace/direction-enter
                                                              :com.wsscode.pathom3.trace/duration      0
                                                              :com.wsscode.pathom3.trace/event-type    :c2
                                                              :com.wsscode.pathom3.trace/parent-id     span-id
                                                              :com.wsscode.pathom3.trace/span-children ()
                                                              :com.wsscode.pathom3.trace/span-id       c2
                                                              :com.wsscode.pathom3.trace/timestamp     123}
                                                             {:com.wsscode.pathom3.trace/direction     :com.wsscode.pathom3.trace/direction-enter
                                                              :com.wsscode.pathom3.trace/duration      0
                                                              :com.wsscode.pathom3.trace/event-type    :c1
                                                              :com.wsscode.pathom3.trace/parent-id     span-id
                                                              :com.wsscode.pathom3.trace/span-children ()
                                                              :com.wsscode.pathom3.trace/span-id       c1
                                                              :com.wsscode.pathom3.trace/timestamp     123})
                   :com.wsscode.pathom3.trace/span-id       span-id
                   :com.wsscode.pathom3.trace/timestamp     123})}))))))

(comment
  (p.trace/trace->tree example-trace)

  (p.trace/normalize-trace example-trace))
