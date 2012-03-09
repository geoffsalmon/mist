(ns mist-test.msgs
  (:use clojure.test)
  (:require
   [mist [msgs :as msgs]]
   [mist.msgs [channels :as channels]]))

(def ^{:dynamic true} *multiplexors* nil)

;; add fixture to close all multiplexors so that udp skts are closed
(defn close-multiplexors [ms]
  (println "close-multiplexors" ms)
  (doseq [m ms]
    (channels/close-channels m)))

(defn close-multiplexors-fixture [f]
  (binding [*multiplexors* (atom [])]
    (try
      (f)
      (finally (close-multiplexors @*multiplexors*)
       ))))

(use-fixtures :each close-multiplexors-fixture)

(defn create-udp-channels
  ([]
     (loop [cnt 10]
       (let [port (+ 10000 (rand-int 20000))]
         (if-let [chans (create-udp-channels port)]
           [port chans]
           (when (> cnt 0)
             (recur (dec cnt) )
             )))))
  ([port]
     (when-let [chans (msgs/create-udp-channels port)]
       (swap! *multiplexors* conj chans)
       chans)))

(deftest creation-and-closing
  (let [[port m1] (create-udp-channels)]
    (is m1)
    (is (nil? (create-udp-channels port)) "Cannot use same port")
    (channels/close-channels m1)
    (let [m2 (create-udp-channels port)]
      (is m2 "can reuse port after closing channels"))))
