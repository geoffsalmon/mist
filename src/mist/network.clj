(ns mist.network
  (:require [aleph [udp :as udp]])
  (:require [lamina [core :as lamina]])
  (:require [gloss [core :as gloss] [io :as glossio]])
  (:require [gloss.data [bytes :as bytes]]))

(defprotocol MsgChannels
  "A mutable set of uniquely numbered channels. Messages can be
dispatch to a channel by number"
  (add-channel [cm channel] [cm channel channel-num]
    "Adds a new channel internal to the multiplexor. If no channel-num
    is specified, a number will be chosen randomly and returned. If a
    channel-num is given but already in use then the channel will be
    replaced and the previous one returned.")
  
  (remove-channel [cm channel-num] "Remove and return a channel.")
  (dispatch-msg [cm channel-num msg]))

(defn- listen-to-channel [cm channel num]
  (lamina/receive-all
   #(lamina/enqueue (:gateway cm) (assoc % :from-channel num))
   channel))

(defrecord ChannelMultiplexor [gateway channels channel-num-fn]
  MsgChannels
  (add-channel [cm channel channel-num]
    (swap! channels
           #(if (nil? (get % channel-num))
              (assoc % channel-num channel)
              (throw (Exception. (str "Channel num " channel-num " already used")))))
    (listen-to-channel cm channel channel-num))

  (add-channel [cm channel]
    ;; add channel to map
    (swap! channels #(assoc %
                       (channel-num-fn %) channel))
    ;; need to save the chosen channel above to pass it to listen-to-channel
    #_(listen-to-channel cm channel))

  (remove-channel [cm channel-num]
    (when-let [ch (@channels channel-num)]
      (lamina/close ch)
      (swap! channels dissoc channel-num)))

  (dispatch-msg [cm channel-num msg]
    (when-let [channel (@channels channel-num)]
      (lamina/enqueue
       channel
       msg))))

(defn- choose-channel-num [channels]
  (loop []
    (let [guess (+ 1000 (rand-int 1000000000))]
      (if (nil? (get channels guess))
        guess
        (recur)))))

(defn channel-multiplexor
  "Creates and returns a generic channel multiplexor. Any messages
  enqueued on the gateway-channel must be maps with a :to-channel
  key. The value corresponding to the :to-channel key determines which
  channel in the multiplexor the enqueued message will be sent
  to. Likewise, any message enqueued in a channel that's added to the
  multiplexor must be a map and will be enqueued on the
  gateway-channel with the appropriate :from-channel added."
  ([gateway-channel]
     (channel-multiplexor choose-channel-num))
  ([gateway-channel channel-num-fn]
      (let [cm (ChannelMultiplexor. gateway-channel (atom {}) channel-num-fn)]
        ;; start receive loop
        (lamina/receive-all
         gateway-channel
         #(dispatch-msg cm (:to-channel %) %))
        cm)))

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

(defn test-cm []
  (let [[c1 c2] (lamina/channel-pair)
        cm (channel-multiplexor c2)]
    [cm c1]))
