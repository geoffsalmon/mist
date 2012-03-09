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
  #_(prn "closing skts" skts)
  (doseq [skt skts]
    (lamina/close skt)))

(defn close-skts-fixture [f]
  (binding [*skts* (atom [])]
    (try
      (f)
      (finally (close-skts @*skts*)))))

(use-fixtures :each close-skts-fixture)

(def localhost "127.0.0.1")

(deftest close-gateway
  (let [[gw gw-port] (create-skt (udp/gateway-options))
        wrapped-gw (udp/wrap-gateway-channel gw)]

    (is (not (lamina/closed? gw)))
    (lamina/close wrapped-gw)
    (is (lamina/closed? gw) "Closing wrapped channel closed underlying udp channel")

    ;; closing outgoing wrapped and gateway channels does not close
    ;; the incoming ones, so the following test fails.
    ;;(is (lamina/drained? wrapped-gw))
    ))

(deftest gateways
  (let [channel-codecs {1 (udp/attach-codec [] (gloss/compile-frame {:val :uint32}))}
        [gw gw-port] (create-skt (udp/gateway-options
                                   (fn [chan]
                                     (channel-codecs (int chan)))))
        gw (udp/wrap-gateway-channel gw)
        codec (gloss/compile-frame
               (gloss/ordered-map :to-channel :uint32
                                  :from-channel :uint32
                                  :val :uint32))
        [skt skt-port] (create-skt {:frame codec})]

    
    ;; test skt->gateway
    ;; send to channel 1 which does have body codec
    (lamina/enqueue skt {:host localhost :port gw-port
                         :message
                         {:to-channel 1 :from-channel 2 :val 3}})

    (let [m (lamina/wait-for-message gw 1000)]
      (is (= (:host m) localhost))
      (is (= (:port m) skt-port))
      (is (= (:to-channel m) 1))
      (is (= (:from-channel m) 2))
      (is (= (:message m) {:val 3}) "channel 1 has body codec"))


    ;; send to channel 20 which doesn't have body codec
    (lamina/enqueue skt {:host localhost :port gw-port
                         :message
                         {:to-channel 20 :from-channel 2 :val 3}})

    (let [m (lamina/wait-for-message gw 1000)]
      (is (= (:host m) localhost))
      (is (= (:port m) skt-port))
      (is (= (:to-channel m) 20))
      (is (= (:from-channel m) 2))
      (is (= (glossio/decode {:val :uint32} (:message m)) {:val 3})
          "channel 2 does not have body codec"))

    ;; test gateway->skt
    
    ;; send from channel 11 which doesn't have body codec
    (lamina/enqueue gw {:host localhost :port skt-port
                        :to-channel 10 :from-channel 11
                        :message (glossio/encode {:val :uint32} {:val 42})})

    (let [m (lamina/wait-for-message skt 1000)
          msg (:message m)]
      (is (= (:host m) localhost))
      (is (= (:port m) gw-port))
      (is (= (:to-channel msg) 10))
      (is (= (:from-channel msg) 11))
      (is (= (:val msg) 42)))


    ;; send from channel 1 which does have channel codec
    (lamina/enqueue gw {:host localhost :port skt-port
                        :to-channel 10 :from-channel 1
                        :message {:val 18}})
    (let [m (lamina/wait-for-message skt 1000)
          msg (:message m)]
      (is (= (:host m) localhost))
      (is (= (:port m) gw-port))
      (is (= (:to-channel msg) 10))
      (is (= (:from-channel msg) 1))
      (is (= (:val msg) 18)))))
