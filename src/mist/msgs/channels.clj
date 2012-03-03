(ns mist.msgs.channels
  (:require [lamina [core :as lamina]]))

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
   channel
   #(lamina/enqueue (:gateway cm) (assoc % :from-channel num))))

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
    #_(listen-to-channel cm channel chosen-channel))

  (remove-channel [cm channel-num]
    ;; not thread safe! the channel that's closed and returned might
    ;; not be the one swapped out of the channels map
    (when-let [ch (@channels channel-num)]
      (lamina/close ch)
      (swap! channels dissoc channel-num)
      ch))

  (dispatch-msg [cm channel-num msg]
    (when-let [channel (@channels channel-num)]
      (lamina/enqueue
       channel
       msg))))


(defn random-channel [min-num max-num]
  (let [spread (- max-num min-num)]
    (fn [channels]
      (loop []
        (let [guess (+ min (rand-int spread))]
          (if (nil? (get channels guess))
            guess
            (recur)))))))

(defn channel-multiplexor
  "Creates and returns a generic channel multiplexor. Any messages
  enqueued on the gateway-channel must be maps with a :to-channel
  key. The value corresponding to the :to-channel key determines which
  channel in the multiplexor the enqueued message will be sent
  to. Likewise, any message enqueued in a channel that's added to the
  multiplexor must be a map and will be enqueued on the
  gateway-channel with the appropriate :from-channel added."
  ([gateway-channel]
     (channel-multiplexor gateway-channel (random-channel 1000 10000)))
  ([gateway-channel channel-num-fn]
      (let [cm (ChannelMultiplexor. gateway-channel (atom {}) channel-num-fn)]
        ;; start receive loop
        (lamina/receive-all
         gateway-channel
         #(dispatch-msg cm (:to-channel %) %))
        cm)))
