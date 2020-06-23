(ns com.wsscode.pathom3.core-test
  (:require [clojure.test :refer [deftest is are run-tests testing]]
            [com.wsscode.pathom3.core :as p]
            [com.wsscode.pathom3.interface.eql :as p.eql]
            [com.wsscode.pathom3.interface.smart-map :as p.sm]
            [com.wsscode.pathom3.connect.indexes :as pci]
            [clojure.spec.alpha :as s]
            [clojure.spec.alpha :as s]))

(p/defresolver foo [env {:keys [foo]}]
  {::pci/input  [:foo]
   ::pci/output [:bar]}
  {:bar (+ foo 3)})

(p/defresolver foo [{:keys [foo]}] :bar
  (+ foo 3))

(p/defresolver foo [{:keys [foo]}] :bar
  {::p/transform wrap-thing}
  (+ foo 3))

(p/resolver 'foo {::p/output-prop :bar}
  (fn []))

(p/defresolver foo [{:keys [foo]}]
  {::pci/output  [:bar]
   ::p/transform wrap-thing}
  {:bar (+ foo 3)})

(p/defresolver foo [env _] :bar
  {:bar (+ foo 3)})

(p/defresolver foo [] :pi 3.14)
(p/defresolver foo [_] :pi 3.14)

;(def env (p/init-env {::p/registry [foo]}))
;
;(p.eql/process env [{[:foo 5] [:bar]}]) ; => 8
;
;(-> (p.sm/smart-map env {:foo 5})
;    :bar) ; => 8
;
;;; for this example imagine we have a lot of geometric resolvers available already
;(let [magic-geo (p.js/js-proxy geometric-ops-env {::g/left 0 ::g/width 50
;                                                  ::g/top  10 ::g/height 50})]
;  (dom/svg
;    (dom/rect magic-geo) ; works, convert :left to x`
;    (dom/circle magic-geo)) ; works anyway, can figure the cx 25 and cy 30
;  )

