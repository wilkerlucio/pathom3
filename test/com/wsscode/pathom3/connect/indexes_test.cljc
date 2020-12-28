(ns com.wsscode.pathom3.connect.indexes-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]))

(deftest merge-oir-test
  (is (= (pci/merge-oir
           '{:a {#{} #{r}}}
           '{:a {#{} #{r2}}})
         '{:a {#{} #{r r2}}})))

(deftest resolver-config-test
  (let [resolver (pco/resolver 'r {::pco/output [:foo]}
                   (fn [_ _] {:foo 42}))
        env      (pci/register resolver)]
    (is (= (pci/resolver-config env 'r)
           '{::pco/input    []
             ::pco/op-name  r
             ::pco/output   [:foo]
             ::pco/provides {:foo {}}})))

  (is (= (pci/resolver-config {} 'r) nil)))

(deftest index-attributes-test
  (testing "combinations"
    (is (= (pci/index-attributes {::pco/op-name 'x
                                  ::pco/input   [:a :b]
                                  ::pco/output  [:c]})
           '{#{:b :a} #:com.wsscode.pathom3.connect.indexes{:attr-id #{:b :a},
                                                            :attr-provides {:c #{x}},
                                                            :attr-input-in #{x}},
             :b #:com.wsscode.pathom3.connect.indexes{:attr-id :b,
                                                      :attr-combinations #{#{:b :a}},
                                                      :attr-input-in #{x}},
             :a #:com.wsscode.pathom3.connect.indexes{:attr-id :a,
                                                      :attr-combinations #{#{:b :a}},
                                                      :attr-input-in #{x}},
             :c #:com.wsscode.pathom3.connect.indexes{:attr-id :c,
                                                      :attr-reach-via {#{:b :a} #{x}},
                                                      :attr-output-in #{x}}}))))

(deftest register-test
  (testing "resolver"
    (let [resolver (pco/resolver 'r {::pco/output [:foo]}
                     (fn [_ _] {:foo 42}))]
      (is (= (pci/register resolver)
             {::pci/index-resolvers  {'r resolver}
              ::pci/index-attributes '{#{}  #::pci{:attr-id       #{}
                                                   :attr-input-in #{r}
                                                   :attr-provides {:foo #{r}}}
                                       :foo #::pci{:attr-id        :foo
                                                   :attr-output-in #{r}
                                                   :attr-reach-via {#{} #{r}}}}
              ::pci/index-io         {#{} {:foo {}}}
              ::pci/index-oir        {:foo {#{} #{'r}}}}))

      (testing "throw error on duplicated name"
        (is (thrown-with-msg?
              #?(:clj AssertionError :cljs js/Error)
              #"Tried to register duplicated resolver: r"
              (pci/register [resolver resolver]))))))

  (testing "mutation"
    (let [mutation (pco/mutation 'm {::pco/output [:foo]
                                     ::pco/params [:bla]}
                                 (fn [_ _] {:foo 42}))]
      (is (= (pci/register mutation)
             {::pci/index-mutations  {'m mutation}
              ::pci/index-attributes {:bla {::pci/attr-id                :bla
                                            ::pci/attr-mutation-param-in #{'m}}
                                      :foo {::pci/attr-id                 :foo
                                            ::pci/attr-mutation-output-in #{'m}}}}))

      (testing "throw error on duplicated name"
        (is (thrown-with-msg?
              #?(:clj AssertionError :cljs js/Error)
              #"Tried to register duplicated mutation: m"
              (pci/register [mutation mutation]))))))

  (testing "multiple globals"
    (let [r1 (pco/resolver 'r {::pco/output [:foo]}
               (fn [_ _] {:foo 42}))
          r2 (pco/resolver 'r2 {::pco/output [:foo2]}
               (fn [_ _] {:foo2 "val"}))]
      (is (= (pci/register [r1 r2])
             {::pci/index-resolvers  {'r  r1
                                      'r2 r2}
              ::pci/index-attributes '{#{}   #::pci{:attr-id       #{}
                                                    :attr-input-in #{r
                                                                     r2}
                                                    :attr-provides {:foo  #{r}
                                                                    :foo2 #{r2}}}
                                       :foo  #::pci{:attr-id        :foo
                                                    :attr-output-in #{r}
                                                    :attr-reach-via {#{} #{r}}}
                                       :foo2 #::pci{:attr-id        :foo2
                                                    :attr-output-in #{r2}
                                                    :attr-reach-via {#{} #{r2}}}}
              ::pci/index-oir        {:foo  {#{} #{'r}}
                                      :foo2 {#{} #{'r2}}}
              ::pci/index-io         {#{} {:foo  {}
                                           :foo2 {}}}}))))

  (testing "adding indexes together"
    (let [r1       (pco/resolver 'r {::pco/output [:foo]}
                     (fn [_ _] {:foo 42}))
          r2       (pco/resolver 'r2 {::pco/output [:foo2]}
                     (fn [_ _] {:foo2 "val"}))
          mutation (pco/mutation 'm {::pco/output [:foo]
                                     ::pco/params [:bla]}
                                 (fn [_ _] {:foo 42}))]
      (is (= (pci/register [(pci/register r1) r2])
             {::pci/index-resolvers  {'r  r1
                                      'r2 r2}
              ::pci/index-attributes '{#{}   #::pci{:attr-id       #{}
                                                    :attr-input-in #{r
                                                                     r2}
                                                    :attr-provides {:foo  #{r}
                                                                    :foo2 #{r2}}}
                                       :foo  #::pci{:attr-id        :foo
                                                    :attr-output-in #{r}
                                                    :attr-reach-via {#{} #{r}}}
                                       :foo2 #::pci{:attr-id        :foo2
                                                    :attr-output-in #{r2}
                                                    :attr-reach-via {#{} #{r2}}}}
              ::pci/index-oir        {:foo  {#{} #{'r}}
                                      :foo2 {#{} #{'r2}}}
              ::pci/index-io         {#{} {:foo  {}
                                           :foo2 {}}}}))

      (is (thrown-with-msg?
            #?(:clj AssertionError :cljs js/Error)
            #"Tried to register duplicated resolver: r"
            (pci/register [(pci/register r1) (pci/register r1)])))

      (is (thrown-with-msg?
            #?(:clj AssertionError :cljs js/Error)
            #"Tried to register duplicated mutation: m"
            (pci/register [(pci/register mutation) (pci/register mutation)]))))))

(deftest attribute-available?-test
  (let [register (pci/register (pco/resolver 'r {::pco/output [:foo]} (fn [_ _] {})))]
    (is (= (pci/attribute-available? register :foo) true))
    (is (= (pci/attribute-available? register :bar) false))))

(deftest reachable-attributes-test
  (testing "all blanks"
    (is (= (pci/reachable-attributes {} {})
           #{})))

  (testing "context data is always in"
    (is (= (pci/reachable-attributes {} {::x {}})
           #{::x})))

  (testing "globals"
    (let [register (pci/register (pbir/constantly-resolver ::x {}))]
      (is (= (pci/reachable-attributes register {})
             #{::x}))))

  (testing "single dependency"
    (let [register (pci/register (pbir/single-attr-resolver ::x ::y inc))]
      (is (= (pci/reachable-attributes register {::x {}})
             #{::x ::y}))))

  (testing "multi dependency"
    (let [register (pci/register (pco/resolver 'xyz
                                   {::pco/input  [::x ::y]
                                    ::pco/output [::z]}
                                   (fn [_ _])))]
      (is (= (pci/reachable-attributes register {::x {}
                                                 ::y {}})
             #{::x ::y ::z})))

    (testing "no go"
      (let [register (pci/register (pco/resolver 'xyz
                                     {::pco/input  [::x ::y]
                                      ::pco/output [::z]}
                                     (fn [_ _])))]
        (is (= (pci/reachable-attributes register {::x {}})
               #{::x}))))

    (testing "extended dependency"
      (let [register (pci/register
                       [(pco/resolver 'xyz
                          {::pco/input  [::x ::y]
                           ::pco/output [::z]}
                          (fn [_ _]))
                        (pbir/alias-resolver ::z ::a)])]
        (is (= (pci/reachable-attributes register {::x {}
                                                   ::y {}})
               #{::x ::y ::z ::a})))

      (let [register (pci/register
                       [(pco/resolver 'xyz
                          {::pco/input  [::x ::y]
                           ::pco/output [::z]}
                          (fn [_ _]))
                        (pbir/alias-resolver ::z ::a)
                        (pco/resolver 'axc
                          {::pco/input  [::a ::x]
                           ::pco/output [::c]}
                          (fn [_ _]))])]
        (is (= (pci/reachable-attributes register {::x {}
                                                   ::y {}})
               #{::x ::y ::z ::a ::c}))))))

(deftest attribute-reachable?-test
  (let [register (pci/register
                   [(pco/resolver 'xyz
                      {::pco/input  [::x ::y]
                       ::pco/output [::z]}
                      (fn [_ _]))
                    (pbir/alias-resolver ::z ::a)
                    (pco/resolver 'axc
                      {::pco/input  [::a ::x]
                       ::pco/output [::c]}
                      (fn [_ _]))])]
    (is (= (pci/attribute-reachable? register {::x {}
                                               ::y {}}
                                     ::c)
           true))

    (is (= (pci/attribute-reachable? register {::x {}
                                               ::y {}}
                                     ::d)
           false))))

(deftest input-set-test
  (is (= (pci/input-set [])
         #{}))

  (is (= (pci/input-set [:foo])
         #{:foo}))

  (is (= (pci/input-set [:foo :bar])
         #{:foo :bar}))

  (is (= (pci/input-set [{:foo [:baz]} :bar])
         #{{:foo #{:baz}} :bar})))
