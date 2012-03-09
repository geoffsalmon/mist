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

  (get-channel [cm channel-num] "Return channel")
  
  (remove-channel [cm channel-num] "Remove and return a channel.")
  
  (dispatch-msg [cm channel-num msg] "Enqueues a message on the
    channel corresponding to the given number. Ignores messages sent to
    non-existent channels.")

  (close-channels [cm] "Close all channels"))

(defrecord ChannelSet [channels channel-num-fn]
  MsgChannels
  (add-channel [cm channel channel-num]
    (dosync
     (if (contains? (ensure channels) channel-num)
       (throw (Exception. (str "Channel num " channel-num " already used")))
       (alter channels assoc channel-num channel)))
    channel-num)

  (add-channel [cm channel]
    (dosync
     ;; choose a channel
     (let [channel-num (channel-num-fn @channels)]
       (if (contains? @channels channel-num)
         (throw (Exception. (str "Channel num " channel-num " already used")))
         ;; add channel to map
         (alter channels assoc channel-num channel))
       channel-num)))

  (get-channel [cm channel-num]
    (get @channels channel-num))

  (remove-channel [cm channel-num]
    (when-let [ch (dosync
                   (when-let [ch (@channels channel-num)]
                     (alter channels dissoc channel-num)
                     ch))]
      (lamina/close ch)
      ch))

  (dispatch-msg [cm channel-num msg]
    (when-let [channel (@channels channel-num)]
      (lamina/enqueue
       channel
       msg)))

  (close-channels [cm]
    (doseq [ch (vals @channels)] (lamina/close ch))))

(defn random-channel [min-num max-num]
  (let [spread (- max-num min-num)]
    (fn [channels]
      (loop []
        (let [guess (+ min-num (rand-int spread))]
          (if (nil? (get channels guess))
            guess
            (recur)))))))

(defn channel-set
  "Creates and returns a channel set. Channel sets contain channels
  with unique channel numbers and support add/remove channels and
  support sending messages to a particular channel. Messages can be
  any object.

  The channel-num-fn is used to select a channel number when
  add-channel is called without a number. It must be a function that
  takes a single argument, the existing channels map, and returns the
  channel number. It is called from within a dosync."
  ([] (channel-set (random-channel 1000 10000)))
  ([channel-num-fn]
     (ChannelSet. (ref {}) channel-num-fn)))

(defn- listen-to-channel [cm channel num]
  ;; TODO: set up on-drained listener to remove channel from the
  ;; multiplexor?
  (lamina/receive-all
   channel
   #(lamina/enqueue (:gateway cm) (assoc % :from-channel num)))
  num)

(defrecord GatewayChannelMultiplexor [gateway channels]
  MsgChannels
  (add-channel [cm channel channel-num]
    (when-let [channel-num (add-channel channels channel channel-num)]
      (listen-to-channel cm channel channel-num)))

  (add-channel [cm channel]
    (when-let [channel-num (add-channel channels channel)]
      (listen-to-channel cm channel channel-num)))

  (get-channel [cm channel-num]
    (get-channel channels channel-num))

  (remove-channel [cm channel-num]
    (remove-channel channels channel-num))

  (dispatch-msg [cm channel-num msg]
    (dispatch-msg channels channel-num msg))

  (close-channels [cm]
    (close-channels channels)
    (lamina/close gateway)))


(defn gateway-multiplexor
  "Creates a channel multiplexor from a channel set and a gateway channel.
  Any msg received from the gateway is dispatched to the channels. Any
  message received from the channels is sent to the gateway.

  To support multiplexing, the type of message is restricted. Any
  message enqueued on the gateway-channel must be a map contain the
  map entry [:to-channel channel-num], where channel-num determines
  which channel the enqueued message will be sent to. Likewise, any
  message enqueued in a channel in the multiplexor must be a map and
  it will be enqueued on the gateway-channel with the
  appropriate :from-channel entry added."
  ([gateway-channel channel-set]
      (let [cm (GatewayChannelMultiplexor. gateway-channel channel-set)]
        ;; start receiving from gateway
        ;; TODO: add on-drained listener?
        (lamina/receive-all
         gateway-channel
         #(dispatch-msg cm (:to-channel %) %))
        cm)))
