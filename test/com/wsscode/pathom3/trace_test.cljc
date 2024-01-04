(ns com.wsscode.pathom3.trace-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [com.wsscode.misc.time :as time]
    [com.wsscode.pathom3.trace :as t]
    [promesa.core :as p]))

(deftest add-signal!-test
  (testing "adds when trace* is present"
    (with-redefs [time/now-ms (constantly 123)]
      (let [trace* (atom [])
            env    {::t/trace* trace*}]
        (t/add-signal! env {::t/log "test"})
        (is (= @trace* [{::t/log            "test"
                         ::t/timestamp      123
                         ::t/parent-span-id nil}])))))

  (testing "does not add when trace* is not present"
    (with-redefs [time/now-ms (constantly 123)]
      (let [trace* (atom [])
            env    {}]
        (t/add-signal! env {::t/log "test"})
        (is (= @trace* []))))))

(deftest open-span!-test
  (testing "opens a span and adds it to the trace"
    (with-redefs [t/new-span-id (constantly "span-id")
                  time/now-ms   (constantly 123)]
      (let [trace* (atom [])
            env    {::t/trace* trace*}]
        (is (= (t/open-span! env {::t/log "test"})
               "span-id"))
        (is (= @trace* [{::t/log            "test"
                         ::t/timestamp      123
                         ::t/parent-span-id nil
                         ::t/attributes     {:com.wsscode.pathom3.path/path []}
                         ::t/span-id        "span-id"}]))))))

(deftest close-span!-test
  (testing "closes a span and adds it to the trace"
    (with-redefs [t/new-span-id (constantly "span-id")
                  time/now-ms   (constantly 123)]
      (let [trace* (atom [])
            env    {::t/trace* trace*}]
        (t/close-span! env "span-id")
        (is (= @trace* [{:com.wsscode.pathom3.trace/close-span-id  "span-id"
                         :com.wsscode.pathom3.trace/parent-span-id nil
                         :com.wsscode.pathom3.trace/timestamp      123
                         :com.wsscode.pathom3.trace/type           :com.wsscode.pathom3.trace/type-close-span}]))))))

(deftest under-span-test
  (testing "returns a new environment setting the context span id"
    (let [env {::t/parent-span-id "span-id"}]
      (is (= (t/under-span env "span-id-2")
             {::t/parent-span-id "span-id-2"})))))

(deftest with-span!-test
  (testing "opens a new span and closes it after the body is executed"
    (with-redefs [t/new-span-id (constantly "span-id")
                  time/now-ms   (constantly 123)]
      (let [trace* (atom [])
            env    {::t/trace* trace*}]
        (t/with-span! [env {::t/env env}]
          (t/log! env {::t/name "test"}))
        (is (= @trace*
               [{:com.wsscode.pathom3.trace/attributes     {:com.wsscode.pathom3.path/path []}
                 :com.wsscode.pathom3.trace/parent-span-id nil
                 :com.wsscode.pathom3.trace/span-id        "span-id"
                 :com.wsscode.pathom3.trace/timestamp      123}
                {:com.wsscode.pathom3.trace/name           "test"
                 :com.wsscode.pathom3.trace/parent-span-id "span-id"
                 :com.wsscode.pathom3.trace/timestamp      123
                 :com.wsscode.pathom3.trace/type           :com.wsscode.pathom3.trace/type-log}
                {:com.wsscode.pathom3.trace/close-span-id  "span-id"
                 :com.wsscode.pathom3.trace/parent-span-id nil
                 :com.wsscode.pathom3.trace/timestamp      123
                 :com.wsscode.pathom3.trace/type           :com.wsscode.pathom3.trace/type-close-span}])))))

  #?(:clj
     (testing "supports async body"
       (let [trace* (atom [])
             env    {::t/trace* trace*}]
         @(t/with-span! [env {::t/env env}]
            (p/do!
              (p/delay 200)
              (t/log! env {::t/name "test"})))
         (is (= 3 (count @trace*)))
         (is (apply < (map ::t/timestamp @trace*)))))))

(deftest log!-test
  (testing "adds a log entry"
    (with-redefs [t/new-span-id (constantly "span-id")
                  time/now-ms   (constantly 123)]
      (let [trace* (atom [])
            env    {::t/trace* trace*}]
        (t/log! env {::t/name "test"})
        (is (= @trace*
               [{:com.wsscode.pathom3.trace/name           "test"
                 :com.wsscode.pathom3.trace/parent-span-id nil
                 :com.wsscode.pathom3.trace/timestamp      123
                 :com.wsscode.pathom3.trace/type           :com.wsscode.pathom3.trace/type-log}]))))))

(deftest set-attributes!-test
  (testing "adds an attribute set entry"
    (with-redefs [t/new-span-id (constantly "span-id")
                  time/now-ms   (constantly 123)]
      (let [trace* (atom [])
            env    {::t/trace* trace*}]
        (t/set-attributes! env :some-attr "value")
        (is (= @trace*
               [{:com.wsscode.pathom3.trace/attributes     {:some-attr "value"}
                 :com.wsscode.pathom3.trace/parent-span-id nil
                 :com.wsscode.pathom3.trace/timestamp      123
                 :com.wsscode.pathom3.trace/type           :com.wsscode.pathom3.trace/type-set-attributes}]))))))

(deftest fold-trace-test
  (testing "closes spans"
    (is (= (t/fold-trace
             [{:com.wsscode.pathom3.trace/attributes     {:com.wsscode.pathom3.path/path []}
               :com.wsscode.pathom3.trace/parent-span-id nil
               :com.wsscode.pathom3.trace/span-id        "span-id"
               :com.wsscode.pathom3.trace/timestamp      123}
              {:com.wsscode.pathom3.trace/close-span-id "span-id"
               :com.wsscode.pathom3.trace/timestamp     1234
               :com.wsscode.pathom3.trace/type          :com.wsscode.pathom3.trace/type-close-span}])
           [{:com.wsscode.pathom3.trace/attributes     {:com.wsscode.pathom3.path/path []},
             :com.wsscode.pathom3.trace/parent-span-id nil,
             :com.wsscode.pathom3.trace/span-id        "span-id",
             :com.wsscode.pathom3.trace/start-time     123,
             :com.wsscode.pathom3.trace/end-time       1234}])))

  (testing "add log to span"
    (is (= (t/fold-trace
             [{:com.wsscode.pathom3.trace/attributes     {:com.wsscode.pathom3.path/path []}
               :com.wsscode.pathom3.trace/parent-span-id nil
               :com.wsscode.pathom3.trace/span-id        "span-id"
               :com.wsscode.pathom3.trace/timestamp      123}
              {:com.wsscode.pathom3.trace/attributes     {:com.wsscode.pathom3.path/path []}
               :com.wsscode.pathom3.trace/name           "test"
               :com.wsscode.pathom3.trace/parent-span-id "span-id"
               :com.wsscode.pathom3.trace/timestamp      124
               :com.wsscode.pathom3.trace/type           :com.wsscode.pathom3.trace/type-log}
              {:com.wsscode.pathom3.trace/close-span-id "span-id"
               :com.wsscode.pathom3.trace/timestamp     125
               :com.wsscode.pathom3.trace/type          :com.wsscode.pathom3.trace/type-close-span}])
           [{:com.wsscode.pathom3.trace/attributes     {:com.wsscode.pathom3.path/path []},
             :com.wsscode.pathom3.trace/parent-span-id nil,
             :com.wsscode.pathom3.trace/span-id        "span-id",
             :com.wsscode.pathom3.trace/start-time     123,
             :com.wsscode.pathom3.trace/events         [{:com.wsscode.pathom3.trace/attributes {:com.wsscode.pathom3.path/path []},
                                                         :com.wsscode.pathom3.trace/name       "test",
                                                         :com.wsscode.pathom3.trace/timestamp  124,
                                                         :com.wsscode.pathom3.trace/type       :com.wsscode.pathom3.trace/type-log}],
             :com.wsscode.pathom3.trace/end-time       125}])))

  (testing "set attributes of span"
    (is (= (t/fold-trace
             [{:com.wsscode.pathom3.trace/attributes     {:com.wsscode.pathom3.path/path []}
               :com.wsscode.pathom3.trace/parent-span-id nil
               :com.wsscode.pathom3.trace/span-id        "span-id"
               :com.wsscode.pathom3.trace/timestamp      123}
              {:com.wsscode.pathom3.trace/attributes     {:some-attr "value"}
               :com.wsscode.pathom3.trace/parent-span-id "span-id"
               :com.wsscode.pathom3.trace/timestamp      123
               :com.wsscode.pathom3.trace/type           :com.wsscode.pathom3.trace/type-set-attributes}
              {:com.wsscode.pathom3.trace/close-span-id "span-id"
               :com.wsscode.pathom3.trace/timestamp     125
               :com.wsscode.pathom3.trace/type          :com.wsscode.pathom3.trace/type-close-span}])
           [{:com.wsscode.pathom3.trace/attributes     {:com.wsscode.pathom3.path/path []
                                                        :some-attr                     "value"}
             :com.wsscode.pathom3.trace/end-time       125
             :com.wsscode.pathom3.trace/parent-span-id nil
             :com.wsscode.pathom3.trace/span-id        "span-id"
             :com.wsscode.pathom3.trace/start-time     123}]))))
