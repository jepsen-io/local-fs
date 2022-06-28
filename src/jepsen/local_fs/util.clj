(ns jepsen.local-fs.util
  "Kitchen sink"
  (:require [clojure.java [shell :as shell]]
            [clojure.tools.logging :refer [info warn]]
            [clojure.test.check [rose-tree :as rose]
                                [generators :as gen]
                                [random :as random]]
            [jepsen [util :as util :refer [name+]]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.util Arrays
                      HexFormat)
           (java.nio.charset StandardCharsets)))

(defn hex->bytes
  "Converts a hex string to a byte array."
  [^String hex]
  (-> (HexFormat/of) (.parseHex hex)))

(defn bytes->hex
  "Converts a byte array to a hex string"
  [^bytes bs]
  (-> (HexFormat/of) (.formatHex bs)))

(def ^:dynamic *sh-trace*
  "If true, logs sh commands."
  false)

(defn sh-escape
  "Converts arguments to strings for sh."
  [arg]
  (cond (string? arg) arg
        :else         (name+ arg)))

(defn sh*
  "Wrapper around clojure.java.sh which takes keywords and other non-strings, a
  terminal options map instead of inline options, and also throws on non-zero
  exit. Returns a map, just like sh does. Example:

      (sh* :echo {:in 2})"
  [& args]
  (let [last-arg    (last args)
        [args opts] (if (map? last-arg)
                      [(drop-last args) last-arg]
                      [args {}])
        sh-args     (concat (map sh-escape args)
                            (mapcat identity opts))
        _           (when *sh-trace*
                      (info (pr-str sh-args)))
        res         (apply shell/sh sh-args)]
    (when-not (zero? (:exit res))
      (throw+ (assoc res :type ::nonzero-exit)
              (str "Shell command " (pr-str sh-args)
                   " returned exit status " (:exit res) "\n"
                   (:out res) "\n"
                   (:err res))))
    res))

(defn sh
  "Like sh*, but returns just stdout."
  [& args]
  (:out (apply sh* args)))

;; Test.check Generators
(defn gen->tree
  "Generates a random value from the given generator, and returns not just the
  value, but the full Rose tree."
  [gen]
  (let [r (random/make-random)]
    (gen/call-gen gen (first (gen/lazy-random-states r)) 10)))

(defn rose->data
  "Converts a rose tree to a seq of [head ... children ...]"
  [rose]
  (cons (rose/root rose) (map rose->data (rose/children rose))))

(defn history->rose
  "You've got a 100,000 operation history which triggers a bug, and want to
  find a smaller history that also reproduces it. This function takes a history
  vector and returns a rose tree of shrunken history vectors from it.

  The end of the history is meaningless: the bug happened already, and
  executing those operations is wasted work. The first part is *also* probably
  meaningless: all that state got overwritten by later operations. We want to
  prune these bits *fast*.

  After that pruning is complete, there should be a short chunk of operations
  with some irrelevant ops scattered in there. To get rid of the irrelevant
  ops, we trim single operations at a time."
  [history]
  (rose/make-rose history
                  (map history->rose
                       (concat
                         ; First, the aggressive pruning: divide the history in
                         ; halves
                         (when (<= 4 (count history))
                           (let [cut (int (/ (count history) 2))]
                             [(subvec history 0 cut)
                              (subvec history cut)]))
                         ; Then delete each element in turn
                         (->> (range (count history))
                              (map (fn [i]
                                     (into (subvec history 0 i)
                                           (subvec history (inc i))))))))))

(defn history->gen
  "Turns a history into a test.check generator using history->rose"
  [history]
  (-> history history->rose gen/gen-pure))
