(ns jepsen.local-fs.shell.workload
  "A combined workload for testing a local filesystem via shell operations: a
  generator, client, and checker which work together."
  (:require [jepsen [generator :as gen]]
            [jepsen.local-fs [util :refer [bytes->hex
                                           hex->bytes]]]
            [jepsen.local-fs.shell [client :refer [client]]
                                   [checker :refer [checker]]]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as g]
                                [properties :as prop]
                                [results :refer [Result]]]))

(def gen-path
  "test.check generator for a random fs-test path. Generates vectors of length
  1 or 2 of \"a\" or \"b\"--we want a real small state space here or we'll
  almost never do anything interesting on the same files."
  (g/vector (g/elements ["a" "b"]) 1 2))

(def data-gen
  "Generates a short string of data to write to a file for an fs test"
  (g/scale #(/ % 100) g/string-alphanumeric))

(def data-gen
  "Generates a short hex string of data to write to a file for an fs test"
  (g/fmap bytes->hex (g/scale #(/ % 100) g/bytes)))

(defn fs-op-gen
  "test.check generator for fs test ops. Generates append, write, mkdir, mv,
  read, etc. Options are:

    :lose-unfsynced-writes    If set, emits operations to lose un-fsynced
                              writes"
  [opts]
  (g/frequency
    (into [[5 (g/let [path gen-path]
                {:f :read, :value [path nil]})]
           [1 (g/let [path gen-path]
                {:f :touch, :value path})]
           [1 (g/let [path gen-path, data data-gen]
                {:f :append, :value [path data]})]
           [1 (g/let [source gen-path, dest gen-path]
                {:f :mv, :value [source dest]})]
           [1 (g/let [path gen-path, data data-gen]
                {:f :write, :value [path data]})]
           [1 (g/let [path gen-path]
                {:f :mkdir, :value path})]
           [1 (g/let [path gen-path]
                {:f :fsync, :value path})]
           [1 (g/let [path gen-path]
                {:f :rm, :value path})]
           [1 (g/let [from gen-path, to gen-path]
                {:f :ln, :value [from to]})]
           [1 (g/let [path gen-path
                      size (->> g/small-integer (g/scale (partial * 0.01)))]
                {:f :truncate, :value [path size]})]]
          (when (:lose-unfsynced-writes opts)
            [[1 (g/return {:f :lose-unfsynced-writes})]]))))

(defn fs-history-gen
  "Generates a whole history"
  [opts]
  (g/scale (partial * 1000)
    (g/vector (fs-op-gen opts))))

(defn workload
  "Constructs a new workload for a test."
  [{:keys [dir db] :as opts}]
  {:client          (client dir db)
   :checker         (checker)
   :test-check-gen  (fs-history-gen opts)})
