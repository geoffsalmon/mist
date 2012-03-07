(ns mist.msgs.udp
  (:require
   [lamina [core :as lamina]]
   [aleph.udp]
   [aleph [formats :as formats]]
   [gloss [core :as gloss] [io :as glossio]]
   [gloss.data [bytes :as bytes]]
   [gloss.core [codecs :as codecs] [protocols :as protocols]])

  (:import
   [org.jboss.netty.channel ChannelException]))

(gloss/defcodec- header-codec
  (gloss/ordered-map :to-channel :uint32 :from-channel :uint32))

(def ^{:private true} header-len 8)

(defn attach-codec [channel codec]
  (vary-meta channel assoc ::codec codec))

(defn udp-socket [opts]
  (try
    ;; is there anyway to let the OS pick the port and
    ;; discover the port number it chose?
    (aleph.udp/udp-socket opts) 
    (catch ChannelException e
      nil)))

(defn wrap-gateway-channel [gateway]
  (let [ch (lamina/channel)]
    (lamina/siphon
     (lamina/map*
      (fn [msg]
        (let [msg (assoc msg :message {:message (:message msg)
                                       :to-channel (:to-channel msg)
                                       :from-channel (:from-channel msg)})]

          (println "sending msg" msg)
          msg))
      ch)
     gateway)

    (lamina/splice
     (lamina/map*
      (fn [msg]
        (println "receiving msg" msg)
        (into msg (:message msg)))
      gateway)
     ch)))

;; TODO: refactor this so that encoders and decoders are created once
;; for each channel registered and then reused, not created for every
;; message.

(defn- gateway-codec [channel-codec-fn]
  (gloss/header
   header-codec
   (fn [header-val]
     (println "header->codec" header-val)
     ;; get body codec from channel metadata
     (let [body-codec
           (or
            (channel-codec-fn header-val)
            ;; if no set channel codec, pass bytes through unchanged
            codecs/identity-codec)]
       (println "returning " body-codec (channel-codec-fn header-val))
       
       (let [read-codec (protocols/compose-callback
                         body-codec
                         (fn [body-val b]
                           (println "compose-callback" body-val)
                           [true (assoc header-val :message body-val) b]))]
         (reify
           protocols/Reader
           (read-bytes [_ b]
             (protocols/read-bytes read-codec b))
           protocols/Writer
           (sizeof [_]
             (protocols/sizeof body-codec))
           (write-bytes [_ buf v]
             (println "write-bytes" v)
             (protocols/write-bytes body-codec buf (:message v)))))))

   (fn [x] (println "body->header" x) x)))

(defn gateway-options
  ([] (gateway-codec (constantly nil)))
  ([channel-codec-fn]
     {:encoder (gateway-codec #(channel-codec-fn (:from-channel %)))
      :decoder (gateway-codec #(channel-codec-fn (:to-channel %)))}))
