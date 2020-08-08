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
(>def ::box (s/keys))

(>defn sqrt [n]
  [number? => number?]
  #?(:clj  (Math/sqrt n)
     :cljs (js/Math.sqrt n)))

(pco/defresolver left [{::keys [right width]}] ::left
  (- right width))

(pco/defresolver right [{::keys [left width]}] ::right
  (+ left width))

(pco/defresolver top [{::keys [bottom height]}] ::top
  (- bottom height))

(pco/defresolver bottom [{::keys [top height]}] ::bottom
  (+ top height))

(pco/defresolver width [{::keys [left right]}] ::width
  (- right left))

(pco/defresolver height [{::keys [top bottom]}] ::height
  (- bottom top))

(pco/defresolver half-width [{::keys [width]}] ::half-width
  (/ width 2))

(pco/defresolver half-height [{::keys [height]}] ::half-height
  (/ height 2))

(pco/defresolver center-x [{::keys [left half-width]}] ::center-x
  (+ left half-width))

(pco/defresolver center-y [{::keys [top half-height]}] ::center-y
  (+ top half-height))

(pco/defresolver midpoint [{::keys [center-x center-y]}] ::midpoint
  {::left center-x
   ::top  center-y})

(pco/defresolver turn-point [{::keys [left top]}] ::turn-point
  {::right  left
   ::bottom top})

(pco/defresolver turn-point2 [{::keys [right bottom]}] ::turn-point
  {::left right
   ::top  bottom})

(pco/defresolver diagonal [{::keys [width height]}] ::diagonal
  (sqrt (+ (* width width) (* height height))))

(def registry
  [left
   right
   top
   bottom
   width
   height
   half-width
   half-height
   center-x
   center-y
   midpoint
   turn-point
   turn-point2
   diagonal
   (pbir/alias-resolver2 ::x ::left)
   (pbir/alias-resolver2 ::y ::top)])

(def geo->svg-registry
  [(pbir/alias-resolver2 :x ::left)
   (pbir/alias-resolver2 :y ::top)
   (pbir/alias-resolver2 :left ::left)
   (pbir/alias-resolver2 :right ::right)
   (pbir/alias-resolver2 :top ::top)
   (pbir/alias-resolver2 :bottom ::bottom)
   (pbir/alias-resolver2 :width ::width)
   (pbir/alias-resolver2 :height ::height)
   (pbir/alias-resolver2 :cx ::center-x)
   (pbir/alias-resolver2 :cy ::center-y)
   (pbir/alias-resolver2 :rx ::half-width)
   (pbir/alias-resolver2 :ry ::half-height)
   (pbir/alias-resolver :rx :r)
   (pbir/alias-resolver :ry :r)])
