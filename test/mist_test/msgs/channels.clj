(ns mist-test.msgs.channels
  (:use clojure.test)
  (:require [mist.msgs [channels :as ch]])
  (:require [lamina [core :as lamina]]))

(def timeout 1000)

(defn create-gw-cm [& opts]
  (let [chan-set (apply ch/channel-set opts)
        [ext gw] (lamina/channel-pair)]
    [ext (ch/gateway-multiplexor gw chan-set)]))

(defn create-channel [cm & opts]
  (let [[c1 c2] (lamina/channel-pair)]
    [c1 (apply ch/add-channel cm c2 opts)]
    ))

(deftest dispatch
  (let [cm (ch/channel-set)
        [c0 _] (create-channel cm 0)
        [c1 _] (create-channel cm 1)]
    (ch/dispatch-msg cm 0 41)
    (ch/dispatch-msg cm 2 42)

    (is (= (lamina/channel-seq c0) [41]))
    (is (empty? (lamina/channel-seq c1)))))

(deftest msg-out
  (let [[gw cm] (create-gw-cm)
        [c1 _] (create-channel cm 0)
        [c2 _] (create-channel cm 1)]

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
  (let [[gw cm] (create-gw-cm)
        [c1 _] (create-channel cm 0)
        [c2 _] (create-channel cm 1)]
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
  (let [[gw cm] (create-gw-cm)
        [c1 _] (create-channel cm 0)

        [c2a c2b] (lamina/channel-pair)]
    (ch/add-channel cm c2b 1)

    (is (nil? (ch/get-channel cm 2)) "non-existent channel")
    (is (= c2b (ch/get-channel cm 1)))

    (is (thrown? Exception (create-channel cm 1)) "channel must have unique nums")
    (is (= c2b (ch/remove-channel cm 1)) "remove returns removed channel")
    (is (nil? (ch/get-channel cm 1)) "get after remove")

    (is (nil? (ch/remove-channel cm 10)) "remove non-existent channel")

    (is (lamina/closed? c2b) "removed channel is closed")
    
    (lamina/enqueue gw {:msg "red" :to-channel 1})

    (let [[c2 _] (create-channel cm 1)]
      (lamina/enqueue gw {:msg "blue" :to-channel 1})
      (is (= (lamina/channel-seq c2)
             [{:msg "blue" :to-channel 1}]) "removed channel can be replaced"))))

(deftest add-remove-random-channels
  (let [[gw cm] (create-gw-cm)
        [c1 n1] (create-channel cm)
        [c2 n2] (create-channel cm)]

    (is (not= n1 n2))
    (is (thrown? Exception (create-channel cm n1)) "channel must have unique nums")
    
    (lamina/enqueue gw {:msg "red" :to-channel n1})
    (lamina/enqueue gw {:msg "blue" :to-channel n2})

    (is (= (lamina/channel-seq c1)
           [{:msg "red" :to-channel n1}]))
    (is (= (lamina/channel-seq c2)
           [{:msg "blue" :to-channel n2}]))

    (ch/remove-channel cm n2)
    (is (= (lamina/channel-seq c2) [nil]))
    (is (lamina/drained? c2))
    
    (lamina/enqueue gw {:msg "red" :to-channel n1})
    (lamina/enqueue gw {:msg "blue" :to-channel n2})
    (is (= (lamina/channel-seq c1)
           [{:msg "red" :to-channel n1}]))))

(deftest channel-nums)

(deftest close
  (let [[gw cm] (create-gw-cm)
        [c1 _] (create-channel cm 0)
        [c2 _] (create-channel cm 1)]

    (is (empty? (lamina/channel-seq gw)))
    (is (empty? (lamina/channel-seq c1)))
    (is (empty? (lamina/channel-seq c2)))

    (ch/close-channels cm)
    (is (= (lamina/channel-seq gw) [nil]))
    (is (= (lamina/channel-seq c1) [nil]))
    (is (= (lamina/channel-seq c2) [nil]))

    (is (lamina/drained? gw))
    (is (lamina/drained? c1))
    (is (lamina/drained? c2))))
