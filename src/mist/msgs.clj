(ns mist.msgs
  (:require
   [mist.msgs
    [channels :as channels]
    [udp :as udp]]
   [lamina [core :as lamina]]))

(defn create-udp-channels [port]
  (let [chan-set (channels/channel-set)
        opts (udp/gateway-options #(channels/get-channel chan-set %))]
    (when-let [gw (udp/udp-socket (assoc opts :port port))]
      (channels/gateway-multiplexor
       (udp/wrap-gateway-channel (lamina/wait-for-result gw))
       chan-set))))
