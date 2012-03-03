(ns mist.msgs.udp
  (:require [lamina [core :as lamina]])
  (:require [gloss [core :as gloss] [io :as glossio]])
  (:require [gloss.data [bytes :as bytes]]))

(gloss/defcodec- header-codec
  (gloss/ordered-map :to-channel :uint32 :from-channel :uint32))

(def ^{:private true} header-len 8)

(defn wrap-udp-gateway
  ""
  [gateway-channel]
  ;; wrap the gateway-channel in both directions to do udp-specific
  ;; encoding/decoding
  (let [[gateway-side multiplexor-side] (lamina/channel-pair)]

    ;; siphon incoming udp packets
    (lamina/siphon
     (->>
      gateway-channel
      ;; filter messages that are too small to have a header
      (lamina/filter* #(>= (:message %) header-len))

      ;; decode udp messages
      (lamina/map*
       #(let [message (:message %)
              header (glossio/decode
                      header-codec
                      (bytes/take-bytes message header-len))]
          (into
           (assoc % :message (bytes/drop-bytes message header-len))
           header))))
     gateway-side)

    ;; siphon outgoing messages 
    (lamina/siphon
     (lamina/map*
      ;; encode and add the header
      #(assoc % :message
              (bytes/concat-bytes
               (glossio/encode header-codec %)
               (:message %)))
      gateway-side)
     gateway-channel)
    
    multiplexor-side))
