(ns mist-test.msgs
  (:use clojure.test)
  (:require
   [mist [msgs :as msgs]]
   [mist.msgs [channels :as channels] [udp :as udp]]
   [gloss [core :as gloss]]
   [lamina [core :as lamina]]))

(def ^{:dynamic true} *multiplexors* nil)

;; add fixture to close all multiplexors so that udp skts are closed
(defn close-multiplexors [ms]
  ;;(println "close-multiplexors" ms)
  (doseq [m ms]
    (channels/close-channels m)))

(defn close-multiplexors-fixture [f]
  (binding [*multiplexors* (atom [])]
    (try
      (f)
      (finally (close-multiplexors @*multiplexors*)
       ))))

(use-fixtures :each close-multiplexors-fixture)

(defn create-udp-multiplexor
  ([]
     (loop [cnt 10]
       (let [port (+ 10000 (rand-int 20000))]
         (if-let [chans (create-udp-multiplexor port)]
           [port chans]
           (when (> cnt 0)
             (recur (dec cnt) )
             )))))
  ([port]
     (when-let [chans (msgs/create-udp-multiplexor port)]
       (swap! *multiplexors* conj chans)
       chans)))

(defn create-channel [cm & opts]
  (let [[c1 c2] (lamina/channel-pair)]
    [c1 (apply msgs/add-channel cm c2 opts)]))

(deftest creation-and-closing
  (let [[port m1] (create-udp-multiplexor)]
    (is m1)
    ;; this next test works, but prints ugly stack trace
    ;;(is (nil? (create-udp-multiplexor port)) "Cannot use same port")
    (channels/close-channels m1)
    (let [m2 (create-udp-multiplexor port)]
      (is m2 "can reuse port after closing channels"))))

(def localhost "127.0.0.1")

(deftest end-to-end
  (let [;; create two channel multiplexors each with their own udp port
        [porta ma] (create-udp-multiplexor)
        [portb mb] (create-udp-multiplexor)

        ;; add channels to each multiplexor
        codec (gloss/ordered-map
               :a :uint32
               :b :uint32)
        [ca0 _] (create-channel ma :num 0 :codec codec)
        [ca1 _] (create-channel ma :num 1 :codec codec)
        [cb0 _] (create-channel mb :num 1)]

    (lamina/enqueue ca0 {:host localhost
                         :port portb
                         :to-channel 1
                         :message {:a 41 :b 42}})

    (let [m (lamina/wait-for-message cb0 1000)]
      (is (= (:host m) localhost))
      (is (= (:port m) porta))
      (is (= (:to-channel m) 1))
      (is (= (:from-channel m) 0))

      ;; send bytebuffer directly back to a different channel
      (lamina/enqueue cb0 {:host localhost
                           :port porta
                           :to-channel 1
                           :message (:message m)}))

    (let [m (lamina/wait-for-message ca1 1000)]
      (is (= (:host m) localhost))
      (is (= (:port m) portb))
      (is (= (:to-channel m) 1))
      (is (= (:from-channel m) 1))
      (is (= (:message m) {:a 41 :b 42})))))
