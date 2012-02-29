(ns mist.kademlia.bittree
  "A bittree maps bitstrings to buckets and also calculates numbers needed for deciding when to split a bucket. Buckets are opaque to the bittree except that they must implement count."
  (:require [mist.kademlia.util :as util]
            [mist.strict :as strict]
            clojure.core.match))

(defn leaf [bucket]
  {:type ::leaf :bucket bucket :size (count bucket)})

(defn branch [child-t child-f]
  {:type ::branch :child-t child-t :child-f child-f :size (+ (:size child-t) (:size child-f))})

(defn- update-internal [update-fun target-bits self-bits depth gap tree]
  "Note: gap is defined as the total size of all buckets closer to self than target"
  (strict/match [tree]
    [{:type ::leaf :bucket bucket}]
    (strict/match [(update-fun target-bits depth gap bucket)]
      [[:single new-bucket]] ; update done
      (leaf new-bucket)
      [[:split new-bucket-t new-bucket-f]] ; have to split first then continue
      (let [tree (branch (leaf new-bucket-t) (leaf new-bucket-f))]
        (recur update-fun target-bits self-bits depth gap tree)))
    [{:type ::branch :child-t child-t :child-f child-f}]
    (let [next-target-bit (first target-bits)
          next-self-bit (first self-bits)
          target-bits (rest target-bits)
          self-bits (rest self-bits)
          depth (inc depth)
          gap (+ gap
                 (cond
                  (= next-target-bit next-self-bit) 0
                  (= next-target-bit 1) (:size child-f)
                  (= next-target-bit 0) (:size child-t)))
          child-t (if (= next-target-bit 0)
                    child-t
                    (update-internal update-fun target-bits self-bits depth gap child-t))
          child-f (if (= next-target-bit 1)
                    child-f
                    (update-internal update-fun target-bits self-bits depth gap child-f))]
      (branch child-t child-f))))

(defn update [update-fun target self tree]
  "Update the bucket at target-bits"
  (update-internal update-fun (util/bits target) (util/bits self) 0 0 tree))

(defn- buckets-internal [prefix-bits suffix-bits tree later-seq]
  (strict/match [tree]
    [{:type ::leaf :bucket bucket}]
    (let [next-item [(reverse prefix-bits) bucket]]
      (lazy-seq (cons next-item later-seq)))
    [{:type ::branch :child-t child-t :child-f child-f}]
    (let [next-bit (first suffix-bits)
          suffix-bits (rest suffix-bits)
          prefix-bits (cons next-bit prefix-bits)
          later-seq (buckets-internal prefix-bits suffix-bits (if (= next-bit 1) child-f child-t) later-seq)]
      (recur prefix-bits suffix-bits (if (= next-bit 1) child-t child-f) later-seq))))

(defn buckets [target tree]
  "Returns a seq of [prefix-bits, bucket] pairs in ascending order of xor distance to target"
  (lazy-seq (buckets-internal nil (util/bits target) tree nil)))
