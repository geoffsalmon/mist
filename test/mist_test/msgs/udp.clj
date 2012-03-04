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


;; is there anyway to let the OS pick the port and discover the port
;; number it chose?
(defn- attempt-bind [port]
  (try
    (aleph.udp/udp-socket {:port port}) 
    (catch ChannelException e
      (println "Unable to bind" port e)
      nil)))

(def ^{:dynamic true} *skts* nil)

(defn create-skt
  "Create and udp socket and return [channel port]. Tries random ports
  until it can successfully bind one."
  []
  (let [port (+ 12000 (rand-int 1000))
        skt (attempt-bind port)]
    (if skt
      (let [skt (lamina/wait-for-result skt)]
        (swap! *skts* conj skt)
        [skt port])
      (recur))))

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
        [skt skt-port] (create-skt)]

    (prn "bound" gw-port "and" skt-port)
    #_(lamina/enqueue gw {:host localhost :port skt-port
                        :to-channel 10 :from-channel 1
                        :message (formats/bytes->channel-buffer "hello")})

    (lamina/enqueue skt {:host localhost :port gw-port
                         :message
                         (formats/bytes->channel-buffer
                          (glossio/encode
                           (gloss/ordered-map :a :uint32 :b :uint32 :c :uint32)
                           {:a 1 :b 2 :c 3}))})

    #_(prn (lamina/wait-for-message skt 5000))
    (prn (lamina/wait-for-message gw 5000))))


(defn test-skt []
  (let [s1 (lamina/wait-for-result (aleph.udp/udp-socket {:port 10000}))
        s2 (lamina/wait-for-result (aleph.udp/udp-socket {:port 10001}))]
    (try
      (lamina/enqueue
       s1
       {:host localhost :port 10001
        :message
        (formats/bytes->channel-buffer
         (glossio/encode
          (gloss/ordered-map :a :uint32 :b :uint32 :c :uint32)
          {:a 1 :b 2 :c 3}))
        #_(formats/bytes->channel-buffer "thello")})

      (let [m (lamina/wait-for-message s2 1000)
            msg (formats/bytes->byte-buffers (:message m))]
        (println "received" m "\n" msg))

      (finally
       (lamina/close s1)
       (lamina/close s2)))))

(defn test-skt2 []
  (let [s (lamina/wait-for-result (aleph.udp/udp-socket {:port 10000}))]
    ;;(lamina/siphon s s)
    (lamina/receive-all
     s
     #(do (prn "Receive" %)
          (lamina/enqueue s %)))
    s))

