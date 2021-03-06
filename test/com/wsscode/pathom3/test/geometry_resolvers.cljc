(ns com.wsscode.pathom3.test.geometry-resolvers
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.operation :as pco]))

(>def ::position-unit number?)
(>def ::left ::position-unit)
(>def ::right ::position-unit)
(>def ::top ::position-unit)
(>def ::bottom ::position-unit)
(>def ::x ::left)
(>def ::y ::top)
(>def ::width ::position-unit)
(>def ::half-width ::width)
(>def ::height ::position-unit)
(>def ::half-height ::height)
(>def ::center-x ::position-unit)
(>def ::center-y ::position-unit)
(>def ::diagonal ::position-unit)
(>def ::point (s/keys :req [(or (and ::left ::top) (and ::right ::bottom))]))
(>def ::midpoint ::point)
(>def ::turn-point ::point)

(>defn sqrt [n]
  [number? => number?]
  #?(:clj  (Math/sqrt n)
     :cljs (js/Math.sqrt n)))

(pco/defresolver left [{::keys [right width]}]
  {::left (- right width)})

(pco/defresolver right [{::keys [left width]}]
  {::right (+ left width)})

(pco/defresolver top [{::keys [bottom height]}]
  {::top (- bottom height)})

(pco/defresolver bottom [{::keys [top height]}]
  {::bottom (+ top height)})

(pco/defresolver width [{::keys [left right]}]
  {::width (- right left)})

(pco/defresolver height [{::keys [top bottom]}]
  {::height (- bottom top)})

(pco/defresolver half-width [{::keys [width]}]
  {::half-width (/ width 2)})

(pco/defresolver half-width-rev [{::keys [half-width]}]
  {::width (* half-width 2)})

(pco/defresolver half-height [{::keys [height]}]
  {::half-height (/ height 2)})

(pco/defresolver center-x [{::keys [left half-width]}]
  {::center-x (+ left half-width)})

(pco/defresolver center-y [{::keys [top half-height]}]
  {::center-y (+ top half-height)})

(pco/defresolver midpoint [{::keys [center-x center-y]}]
  {::midpoint {::left center-x
               ::top  center-y}})

(pco/defresolver turn-point [{::keys [left top]}]
  {::turn-point {::right  left
                 ::bottom top}})

(pco/defresolver turn-point2 [{::keys [right bottom]}]
  {::turn-point {::left right
                 ::top  bottom}})

(pco/defresolver diagonal [{::keys [width height]}]
  {::diagonal (sqrt (+ (* width width) (* height height)))})

(def registry
  [left
   right
   top
   bottom
   width
   height
   half-width
   half-width-rev
   half-height
   center-x
   center-y
   midpoint
   turn-point
   turn-point2
   diagonal
   (pbir/equivalence-resolver ::x ::left)
   (pbir/equivalence-resolver ::y ::top)])

(def geo->svg-registry
  [(pbir/equivalence-resolver :left ::left)
   (pbir/equivalence-resolver :right ::right)
   (pbir/equivalence-resolver :top ::top)
   (pbir/equivalence-resolver :bottom ::bottom)
   (pbir/equivalence-resolver :x ::x)
   (pbir/equivalence-resolver :y ::y)
   (pbir/equivalence-resolver :width ::width)
   (pbir/equivalence-resolver :height ::height)
   (pbir/equivalence-resolver :cx ::center-x)
   (pbir/equivalence-resolver :cy ::center-y)
   (pbir/equivalence-resolver :rx ::half-width)
   (pbir/equivalence-resolver :ry ::half-height)
   (pbir/alias-resolver :rx :r)
   (pbir/alias-resolver :ry :r)])

(def full-registry [registry geo->svg-registry])

(comment
  (map (comp ::pco/op-name pco/operation-config) (flatten geo->svg-registry))

  (pbir/alias-resolver :foo/id :foo-other/id))
