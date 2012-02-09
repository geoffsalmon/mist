(ns mist.kademlia.bittree
  "A bittree maps bitstrings to buckets and also calculates numbers needed for deciding when to split a bucket. Buckets are opaque to the bittree except that they must implement count."
  (:use [clojure.core.match :only [match]]))

(defn leaf [bucket]
  {:bucket bucket :size (count bucket)})

(defn branch [child-t child-f]
  {:child-t child-t :child-f child-f :size (+ (:size child-t) (:size child-f))})

(defn ^{:private true}
  update-internal [update-fun target-bits self-bits depth gap tree]
  "Note: gap is defined as the total size of all buckets closer to self-bits than target-bits"
  (match [tree]
         [{:bucket bucket}]
         (match [(update-fun target-bits depth gap bucket)]
                [[:single bucket]] ; update done
                (leaf bucket)
                [[:split bucket-t bucket-f]] ; have to split first then continue
                (let [tree (branch (leaf bucket-t) (leaf bucket-f))]
                  (recur target-bits update-fun self-bits depth gap tree)))
         [{:child-t child-t :child-f child-f}]
         (let [next-target-bit (first target-bits)
               next-self-bit (first self-bits)
               target-bits (rest target-bits)
               self-bits (rest self-bits)
               depth (inc depth)
               gap (+ gap
                      (cond
                       (= next-target-bit next-self-bit) 0
                       (= next-target-bit true) (:size child-t)
                       (= next-target-bit false) (:size child-f)))
               child-t (if next-target-bit
                         (update-internal update-fun target-bits self-bits depth gap child-t)
                         child-t)
               child-f (if next-target-bit
                         child-f
                         (update-internal update-fun target-bits self-bits depth gap child-f))]
           (branch child-t child-f))))

(defn update [update-fun target-bits self-bits tree]
  "Update the bucket at target-bits"
  (update-internal update-fun target-bits self-bits 0 0 tree))

(defn ^{:private true}
  buckets-internal [prefix-bits suffix-bits tree rest]
  (match [tree]
         [{:bucket bucket}]
         (let [next-item [(reverse prefix-bits) bucket]]
           (lazy-seq next-item rest))
         [{:child-t child-t :child-f child-f}]
         (let [next-bit (first suffix-bits)
               suffix-bits (rest suffix-bits)
               prefix-bits (cons next-bit prefix-bits)
               rest (buckets-internal prefix-bits suffix-bits (if next-bit child-f child-t) rest)]
           (recur prefix-bits suffix-bits (if next-bit child-t child-f rest)))))

(defn buckets [target-bits tree]
  "Returns a seq of [prefix-bits, bucket] pairs in ascending order of xor distance to target-bits"
  (buckets-internal target-bits () tree nil))
