(ns mist.strict
  (:require clojure.core.match))

(defmacro match [pattern & args]
  `(let [pattern# ~pattern]
     (clojure.core.match/match
      pattern#
      ~@args
      :else (let [msg# (str "Could not match " pattern#)]
              (throw (new Exception msg#))))))
