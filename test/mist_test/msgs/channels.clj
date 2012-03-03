(ns mist-test.msgs.channels
  (:use clojure.test)
  (:require [mist.msgs [channels :as ch]])
  (:require [lamina [core :as lamina]]))

(def timeout 1000)

(defn create-cm [& opts]
  (let [[ext gw] (lamina/channel-pair)]
    [ext (apply ch/channel-multiplexor gw opts)]))

(defn create-channel [cm & opts]
  (let [[c1 c2] (lamina/channel-pair)]
    (apply ch/add-channel cm c2 opts)
    c1))

(deftest msg-out
  (let [[gw cm] (create-cm)
        c1 (create-channel cm 0)
        c2 (create-channel cm 1)]

    (is (empty? (lamina/channel-seq c1)))
    (lamina/enqueue c1 {:msg "hi"})
    (is (= (lamina/wait-for-message gw timeout)) {:msg "hi" :from-channel 0})
    (is (empty? (lamina/channel-seq gw)))
    
    (lamina/enqueue c1 {:msg "red"})
    (lamina/enqueue c2 {:msg "green"})
    (lamina/enqueue c1 {:msg "blue"})
    (is (= (lamina/channel-seq gw)
           [{:msg "red" :from-channel 0}
            {:msg "green" :from-channel 1}
            {:msg "blue" :from-channel 0}]))))

(deftest msg-in
  (let [[gw cm] (create-cm)
        c1 (create-channel cm 0)
        c2 (create-channel cm 1)]
    (is (empty? (lamina/channel-seq c1)))
    (is (empty? (lamina/channel-seq c2)))

    ;; messages with no to-channel or to missing channels will be
    ;; ignored
    (lamina/enqueue gw {:msg "hi"})
    (lamina/enqueue gw {:msg "hi" :to-channel 3})

    (is (empty? (lamina/channel-seq c1)))
    (is (empty? (lamina/channel-seq c2)))

    (lamina/enqueue gw {:msg "hi" :to-channel 0})
    (is (= (lamina/channel-seq c1) [{:msg "hi" :to-channel 0}]))
    (is (empty? (lamina/channel-seq c2)))

    (lamina/enqueue gw {:msg "red" :to-channel 0})
    (lamina/enqueue gw {:msg "green" :to-channel 1})
    (lamina/enqueue gw {:msg "blue" :to-channel 0})
    (lamina/enqueue gw {:msg "yellow" :to-channel 0})

    (is (= (lamina/channel-seq c1)
           [{:msg "red" :to-channel 0}
            {:msg "blue" :to-channel 0}
            {:msg "yellow" :to-channel 0}]))
    (is (= (lamina/channel-seq c2)
           [{:msg "green" :to-channel 1}]))))

(deftest add-remove-channels
  (let [[gw cm] (create-cm)
        c1 (create-channel cm 0)

        [c2a c2b] (lamina/channel-pair)]
    (ch/add-channel cm c2b 1)
    (is (thrown? Exception (create-channel cm 1)) "channel must have unique nums")
    (is (= c2b (ch/remove-channel cm 1)) "remove returns removed channel")

    (is (lamina/closed? c2b) "removed channel is closed")
    
    (lamina/enqueue gw {:msg "red" :to-channel 1})

    (let [c2 (create-channel cm 1)]
      (lamina/enqueue gw {:msg "blue" :to-channel 1})
      (is (= (lamina/channel-seq c2)
             [{:msg "blue" :to-channel 1}]) "removed channel can be replaced"))))

(deftest channel-nums)
