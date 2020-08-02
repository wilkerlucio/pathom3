(ns com.wsscode.pathom3.connect.operation.protocols)

(defprotocol IOperation
  (-operation-config [this])
  (-operation-type [this]))

(defprotocol IResolver
  (-resolve [this env input]))

(defprotocol IMutate
  (-mutate [this env params]))
