(ns mist.msgs.channels
  (:require [lamina [core :as lamina]]))

(defprotocol MsgChannels
  "A mutable set of uniquely numbered channels. Messages can be
dispatch to a channel by number"
  (add-channel [cm channel] [cm channel channel-num]
    "Adds a new channel internal to the multiplexor. If no channel-num
    is specified, a number will be chosen randomly and returned. If a
    channel-num is given but already in use then an Exception is
    thrown. Returns the new channels number.")
  
  (remove-channel [cm channel-num] "Remove and return a channel.")
  
  (dispatch-msg [cm channel-num msg] "Enqueues a message on the
  channel corresponding to the given number. Ignores messages sent to
  non-existent channels."))

(defn- listen-to-channel [cm channel num]
  (lamina/receive-all
   channel
   #(lamina/enqueue (:gateway cm) (assoc % :from-channel num)))
  num)

(defrecord GatewayChannelMultiplexor [gateway channels channel-num-fn]
  MsgChannels
  (add-channel [cm channel channel-num]
    (dosync
     (if (contains? (ensure channels) channel-num)
       (throw (Exception. (str "Channel num " channel-num " already used")))
       (alter channels assoc channel-num channel)))
    (listen-to-channel cm channel channel-num))

  (add-channel [cm channel]
    (let [channel-num (ref nil)]
      (dosync
       ;; choose a channel
       (ref-set channel-num (channel-num-fn @channels))
       ;; add channel to map
       (alter channels assoc @channel-num channel))
      (listen-to-channel cm channel @channel-num)))

  (remove-channel [cm channel-num]
    (when-let [ch
          (dosync
           (when-let [ch (@channels channel-num)]
             (alter channels dissoc channel-num)
             ch))]
      (lamina/close ch)
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
        (let [guess (+ min-num (rand-int spread))]
          (if (nil? (get channels guess))
            guess
            (recur)))))))

(defn gateway-multiplexor
  "Creates and returns a channel multiplexor with a gateway
  channel.

  Any messages enqueued on the gateway-channel must be maps with
  a :to-channel key. The value corresponding to the :to-channel key
  determines which channel in the multiplexor the enqueued message
  will be sent to. Likewise, any message enqueued in a channel in the
  multiplexor must be a map and will be enqueued on the
  gateway-channel with the appropriate :from-channel added.

  The channel-num-fn is used to select a channel number when
  add-channel is called without a number. It must be a function that
  takes a single argument, the existing channels map, and returns the
  channel number. It is called from within a dosync."
  ([gateway-channel]
     (gateway-multiplexor gateway-channel (random-channel 1000 10000)))
  ([gateway-channel channel-num-fn]
      (let [cm (GatewayChannelMultiplexor. gateway-channel (ref {}) channel-num-fn)]
        ;; start receiving from gateway
        (lamina/receive-all
         gateway-channel
         #(dispatch-msg cm (:to-channel %) %))
        cm)))
