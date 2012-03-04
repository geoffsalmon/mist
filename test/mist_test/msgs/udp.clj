(ns mist-test.msgs.udp
  (:use clojure.test)
  (:require [mist.msgs [udp :as udp]])
  (:require
   [aleph.udp]
   [aleph [formats :as formats]]
   [lamina [core :as lamina]]
   [gloss [core :as gloss] [io :as glossio]]
   [gloss.data [bytes :as bytes]])
  (:import
   [org.jboss.netty.channel ChannelException]))


(def ^{:dynamic true} *skts* nil)

(defn create-skt
  "Create and udp socket and return [channel port]. Tries random ports
  until it can successfully bind one."
  ([] (create-skt {}))
  ([opts]
     (loop []
       (let [port (+ 12000 (rand-int 1000))]
         (if-let [skt
                  (try
                    ;; is there anyway to let the OS pick the port and
                    ;; discover the port number it chose?
                    (aleph.udp/udp-socket (assoc opts :port port)) 
                    (catch ChannelException e
                      (println "Unable to bind" port e)
                      nil))]
           (let [skt (lamina/wait-for-result skt)]
             ;; save skt so it can be automatically closed
             (swap! *skts* conj skt)
             [skt port])
           (recur))))))

;; add fixture to close all udp skts
(defn close-skts [skts]
  (prn "closing skts" skts)
  (doseq [skt skts]
    (lamina/close skt)))

(defn close-skts-fixture [f]
  (binding [*skts* (atom [])]
    (try
      (f)
      (finally (close-skts @*skts*)))))

(use-fixtures :each close-skts-fixture)

(def localhost "127.0.0.1")

(deftest gateways
  (let [[gw gw-port] (create-skt)
        gw (udp/wrap-udp-gateway gw)
        codec (gloss/compile-frame
               (gloss/ordered-map :to-channel :uint32
                                  :from-channel :uint32
                                  :val :uint32))
        [skt skt-port] (create-skt {:frame codec})]

    ;; test skt->gateway
    (lamina/enqueue skt {:host localhost :port gw-port
                         :message
                         {:to-channel 1 :from-channel 2 :val 3}})

    (let [m (lamina/wait-for-message gw 1000)]
      (is (= (:host m) localhost))
      (is (= (:port m) skt-port))
      (is (= (:to-channel m) 1))
      (is (= (:from-channel m) 2))
      (is (= (glossio/decode {:val :uint32} (:message m)) {:val 3})))

    (lamina/enqueue gw {:host localhost :port skt-port
                        :to-channel 10 :from-channel 11
                        :message (glossio/encode {:val :uint32} {:val 42})})

    ;; test gateway->skt
    (let [m (lamina/wait-for-message skt 1000)
          msg (:message m)]
      (is (= (:host m) localhost))
      (is (= (:port m) gw-port))
      (is (= (:to-channel msg) 10))
      (is (= (:from-channel msg) 11))
      (is (= (:val msg) 42)))))
