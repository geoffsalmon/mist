(ns mist-test.kademlia.bittree
  (:use clojure.test)
  (:require [clojurecheck.core :as cc]
            [mist.kademlia.conf :as conf]
            [mist.kademlia.bittree :as bittree]
            [clojure.math.numeric-tower :as math]))

(def max-hash
  (- (math/expt 2 conf/bits-per-hash) 1))

(def cc-hash
  (cc/int :lower 0 :upper max-hash))

; for testing we use buckets that look like [[suffix-bits hash] [suffix-bits hash] ...]

(def max-bucket-size 3)

(defn filter-by-next-bit [bucket bit]
  (for [[suffix-bits hash] bucket
        :when (= bit (first suffix-bits))]
    [(rest suffix-bits) hash]))

(defn add-to-bucket [suffix-bits depth hash bucket]
  (if (or (< (count bucket) max-bucket-size) (>= depth conf/bits-per-hash))
    [:single (conj bucket [suffix-bits hash])]
    (let [bucket-f (filter-by-next-bit bucket 0)
          bucket-t (filter-by-next-bit bucket 1)]
      [:split bucket-t bucket-f])))

(defn add-to-tree [hash tree]
  (bittree/update
   (fn [suffix-bits depth gap bucket] (add-to-bucket suffix-bits depth hash bucket))
   hash
   hash ; don't really care about self for this
   tree))

(defn tree-of-hashes [hashes]
  (reduce #(add-to-tree %2 %1) (bittree/leaf []) hashes))

(defn bucket-distances [target [_ bucket]]
  (for [[_ hash] bucket] (bit-xor target hash)))

(defn bucket-contains [target [_ bucket]]
  (some (fn [[_ hash]] (= target hash)) bucket))

(defn bucket-count [[_ bucket]]
  (count bucket))

(deftest bucket-order
  (cc/property
    "(bittree/buckets target tree) should return buckets in ascending order of xor distance from the bucket prefix to target"
    [hashes (cc/list cc-hash)
     target cc-hash]
    (let [distances-before (sort (map #(bit-xor target %) hashes))
          tree (tree-of-hashes hashes)
          buckets (bittree/buckets target tree)
          distances-after (mapcat #(sort (bucket-distances target %)) buckets)]
      (is (= distances-before distances-after)))))

(deftest update-gap
  (cc/property
    "(bittree/update update-fn target self tree) should pass the correct gap size to update-fn"
    [hashes (cc/list cc-hash)
     self cc-hash
     target cc-hash]
    (let [tree (tree-of-hashes (conj hashes target))
          buckets (bittree/buckets self tree)
          buckets-nearer (take-while #(not (bucket-contains target %)) buckets)
          correct-gap (reduce + 0 (map bucket-count buckets-nearer))
          gap-atom (atom nil)]
      (bittree/update
       (fn [_ _ gap bucket]
         (compare-and-set! gap-atom nil gap)
         [:single bucket]) target self tree)
      (is (= @gap-atom correct-gap)))))
