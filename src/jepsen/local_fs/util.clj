(ns jepsen.local-fs.util
  "Kitchen sink"
  (:require [clojure.java [shell :as shell]]
            [clojure.tools.logging :refer [info warn]]
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
