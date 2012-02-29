(ns mist.kademlia.util
  (:require [mist.kademlia.conf :as conf]
            clojure.core.match)
  (:import (java.security MessageDigest)))

(defn sha1 [string]
  (let [bytes (.digest (MessageDigest/getInstance conf/hash) (.getBytes string))]
    (reduce (fn [acc byte] (+ byte (* 256 acc))) bytes)))

(defn- bits-internal [hash num-bits]
  (if (= num-bits 0)
    nil
    (let [bit (rem hash 2)
          hash (bit-shift-right hash 1)
          num-bits (- num-bits 1)]
      (cons bit (bits-internal hash num-bits)))))

(defn bits [hash]
  (reverse (bits-internal hash conf/bits-per-hash)))
