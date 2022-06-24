(ns jepsen.local-fs.util
  "Kitchen sink"
  (:require [clojure.java [shell :as shell]]
            [jepsen [util :as util :refer [name+]]]
            [slingshot.slingshot :refer [try+ throw+]]))

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
        res         (apply shell/sh (concat (->> args
                                                 (map sh-escape))
                                            (mapcat identity opts)))]
    (when-not (zero? (:exit res))
      (throw+ (assoc res :type ::nonzero-exit)
              (str "Shell command " (pr-str args)
                   " returned exit status " (:exit res) "\n"
                   (:out res) "\n"
                   (:err res))))
    res))

(defn sh
  "Like sh*, but returns just stdout."
  [& args]
  (:out (apply sh* args)))
