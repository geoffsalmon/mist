(ns mist.msgs.udp
  (:require [lamina [core :as lamina]])

  (:require [aleph [formats :as formats]])
  (:require [gloss [core :as gloss] [io :as glossio]])
  (:require [gloss.data [bytes :as bytes]]))

(gloss/defcodec- header-codec
  (gloss/ordered-map :to-channel :uint32 :from-channel :uint32))

(def ^{:private true} header-len 8)

;; wrap aleph.core/wrap-endpoint [ch receive-fn enqueue-fn]

(defn wrap-udp-gateway
  ""
  [gateway-channel]
  ;; wrap the gateway-channel in both directions to do udp-specific
  ;; encoding/decoding. This could be simplified if we could compile a
  ;; frame that would parse the header and leave the rest of the
  ;; message as byte buffers and pass that to the udp-socket when it's
  ;; created. I haven't figured out an appropriate frame yet though.
  (let [outgoing (lamina/channel)]
    ;; siphon messages from the new outgoing channel to the udp gateway
    (lamina/siphon
     (lamina/map*
      ;; encode and add the header
      #(assoc % :message
              ;; Convert byte buffers to channel-buffer
              (formats/bytes->channel-buffer
               (bytes/concat-bytes
                (glossio/encode header-codec %)
                (:message %))))
      outgoing)
     gateway-channel)

    (lamina/splice
     (->>
      gateway-channel

      ;; UDP channels seem to produce a message as a
      ;; channel-buffer. Convert to buf seq.
      (lamina/map*
       #(assoc % :message
               (bytes/create-buf-seq (formats/bytes->byte-buffers (:message %)))))

      ;; filter messages that are too small to have a header
      (lamina/filter* #(>= (bytes/byte-count (:message %)) header-len))

      ;; decode udp messages
      (lamina/map*
       #(let [message (:message %)
              header (glossio/decode
                      header-codec
                      (bytes/take-bytes message header-len))]
          (into
           ;; replace message with remainder of bytes
           (assoc % :message (bytes/drop-bytes message header-len))
           header))))
     outgoing)))
