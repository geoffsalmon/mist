(ns mist.msgs.udp
  (:require
   [mist.msgs
    [channels :as channels]]
   [lamina [core :as lamina]]
   [aleph.udp]
;;   [aleph [formats :as formats]]
   [gloss [core :as gloss] #_[io :as glossio]]
;;   [gloss.data [bytes :as bytes]]
   [gloss.core [codecs :as codecs] [protocols :as protocols]])

  (:import
   [org.jboss.netty.channel ChannelException]))

(gloss/defcodec- header-codec
  (gloss/ordered-map :to-channel :uint32 :from-channel :uint32))

(def ^{:private true} header-len 8)

(defn udp-socket [opts]
  (try
    ;; is there anyway to let the OS pick the port and
    ;; discover the port number it chose?
    (aleph.udp/udp-socket opts) 
    (catch ChannelException e
      nil)))

(defn wrap-gateway-channel [gateway]
  (let [ch (lamina/channel)
        ch->gw (lamina/map*
                (fn [msg]
                  (let [msg (assoc msg :message {:message (:message msg)
                                                 :to-channel (:to-channel msg)
                                                 :from-channel (:from-channel msg)})]
                    msg))
                ch)]
    
    (lamina/siphon
     ch->gw
     gateway)

    ;; ensure gateway channel is closed when wrapping channel is
    (lamina/on-drained ch->gw #(lamina/close gateway))

    (lamina/splice
     (lamina/map*
      (fn [msg]
        (into msg (:message msg)))
      gateway)
     ch)))

(defn- channel-encoder [body-codec]
  (let [writer (reify
                 protocols/Writer
                 (sizeof [_]
                   (protocols/sizeof body-codec))
                 (write-bytes [_ buf v]
                   (protocols/write-bytes body-codec buf (:message v))))]
    (fn [header-val]
      writer)))

(defn- channel-decoder [body-codec]
  ;; TODO: Is calling compose-callback for every incoming mesasge
  ;; slow? Is there a better way? It seems to be required when using
  ;; gloss/header because the value of the header, the to-channel and
  ;; from-channel, needs to be added to the decoded
  ;; message. So.. maybe don't use the header codec when receiving
  ;; packets. Or maybe memoize this fn?
  (fn [header-val]
    (protocols/compose-callback
     body-codec
     (fn [body-val b]
       [true (assoc header-val :message body-val) b]))))

(def ^{:private true} generic-encoder (channel-encoder codecs/identity-codec))
(def ^{:private true} generic-decoder (channel-decoder codecs/identity-codec))

(defn- gateway-codec [generic-codec channel-codec-fn]
  (gloss/header
   header-codec
   (fn [header-val]
     ;; get body codec from channel metadata
     ((or (channel-codec-fn header-val) generic-codec) header-val))
   identity))

(defn attach-codec
  "Attach a codec to a channel and returns the new channel. Adds to
  the channels metadata."
  [channel codec]
  (let [codec (gloss/compile-frame codec)]
    (-> channel
        (vary-meta assoc ::encoder (channel-encoder codec))
        (vary-meta assoc ::decoder (channel-decoder codec)))))

(defn gateway-options
  ([] (gateway-options (constantly nil)))
  ([get-channel]
     {:encoder (gateway-codec generic-encoder #(::encoder (meta (get-channel (:from-channel %)))))
      :decoder (gateway-codec generic-decoder #(::decoder (meta (get-channel (:to-channel %)))))}))
