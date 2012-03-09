(ns mist.msgs
  (:require
   [mist.msgs
    [channels :as channels]
    [udp :as udp]]
   [lamina [core :as lamina]]))

(defn add-channel [multiplexor ch & {:keys [codec num]}]
  (let [;; optionally attach codec to channel
        ch (if codec
             (udp/attach-codec ch codec)
             ch)]
    (if num
      (channels/add-channel multiplexor ch num)
      (channels/add-channel multiplexor ch))))

(defn create-udp-multiplexor [port]
  (let [chan-set (channels/channel-set)
        opts (udp/gateway-options #(channels/get-channel chan-set %))]
    (when-let [gw (udp/udp-socket (assoc opts :port port))]
      (channels/gateway-multiplexor
       (udp/wrap-gateway-channel (lamina/wait-for-result gw))
       chan-set))))
