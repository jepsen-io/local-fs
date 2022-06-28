(ns jepsen.local-fs
  "Main entry point for local FS testing. Handles CLI arguments, launches
  tests."
  (:require [clojure [edn :as edn]
                     [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [core :as jepsen]
                    [generator :as gen]
                    [store :as store]
                    [tests :as tests]]
            [jepsen.local-fs.shell.workload :as shell]
            [jepsen.local-fs.db [dir :as db.dir]
                                [lazyfs :as db.lazyfs]]
            [clojure.test.check :as tc]
            [clojure.test.check [generators :as tc.gen]
                                [properties :as prop]
                                [results :refer [Result]]])
  (:import (java.net URLEncoder)
           (java.io PushbackReader)
           (io.lacuna.bifurcan Set)))

(def dbs
  "A map of DB names to functions that take CLI options and build DBs."
  {:dir    db.dir/db
   :lazyfs db.lazyfs/db})

(defn stats-checker
  "A stats checker that never fails"
  []
  (reify checker/Checker
    (check [this test history opts]
      (-> (checker/check (checker/stats) test history opts)
          (assoc :valid? true)))))

(defrecord TapeGen [ops]
  gen/Generator
  (op [this test context]
      (when-let [op (first ops)]
        (let [p      (:process op)
              thread (gen/process->thread context p)
              free-threads ^Set (:free-threads context)]
          ; Only emit an op if the process is free to execute it.
          (if (.contains free-threads thread)
            [(gen/fill-in-op op context) (TapeGen. (next ops))]
            [:pending this]))))

  (update [this test context event]
          this))

(defn history-file->gen
  "Takes a filename and returns a generator from it."
  [file]
  (let [; First, load the critical data we need from disk
        ops (with-open [file (java.io.PushbackReader.
                               (io/reader file))]
              (->> (repeatedly #(edn/read {:eof ::eof} file))
                   (take-while (complement #{::eof}))
                   (keep (fn [op]
                           (when (= :invoke (:type op))
                             ; This won't perfectly reproduce the concurrency
                             ; structure of the original history since we have
                             ; no control over how long operations take, but it
                             ; might get us kinda close?
                             (select-keys op [:process :f :value]))))
                   vec))]
    ; Now construct a generator which zips through this history, dispatching
    ; ops only when those processes are free.
    (TapeGen. ops)))

(defn shell-test
  "Takes CLI options and constructs a test for the shell workload."
  [opts]
  (let [workload (shell/workload opts)
        db       ((dbs (:db opts)) opts)]
    (merge tests/noop-test
           opts
           {:name   (str "shell "
                         (name (:db opts))
                         (when (:version opts)
                           (str " " (:version opts))))
            :ssh    {:dummy? true}
            :nodes  ["local"]
            :db      db
            :checker (checker/compose
                       {:perf       (checker/perf)
                        :stats      (stats-checker)
                        :exceptions (checker/unhandled-exceptions)
                        :workload   (:checker workload)})
            :client         (:client workload)
            :generator (->> (if-let [file (:history opts)]
                              (history-file->gen file)
                              (->> workload
                                   :test-check-gen
                                   tc.gen/sample-seq
                                   (mapcat identity)
                                   (gen/time-limit (:time-limit opts))))
                            gen/clients)})))

(defn test-check-gen
  "Takes CLI options and returns a test.check generator."
  [opts]
  (let [workload (shell/workload opts)]
    (:test-check-gen workload)))

(def cli-opts
  "Common CLI options."
  [[nil "--db DB" "What DB (filesystem) should we mount?"
    :default :dir
    :parse-fn keyword
    :validate [dbs (cli/one-of dbs)]]

   [nil "--dir DIR" "What directory should we use for our mount point?"
    :default "data"]

   [nil "--history FILE" "Run a history taken directly from an EDN file. This
    allows you to shrink a history by hand, in case test.check's shrinking
    finds a very long example and gets stuck. Just copy history.edn from a
    test, edit it with a text editor, save it, and run it with `lein run
    test --history custom.edn`. This won't shrink at all, sadly."]

   ["-v" "--version VERSION" "A version string, passed to the DB. For lazyfs, this controls the git commit we check out."]])

(def quickcheck-cmd
  "A CLI command for quickcheck-style testing. Generates histories using
  test.check and shrinks failing cases."
  {"quickcheck"
   {:opt-spec cli-opts
    :usage    "Runs quickcheck-style tests. Generates histories using test.check and shrinks failing cases."
    :run
    (fn run [{:keys [options]}]
      (let [prop (prop/for-all [history (test-check-gen options)]
                   (let [test (-> (shell-test options)
                                  (assoc :generator
                                         (->> history
                                              ;(gen/delay 2)
                                              gen/clients)
                                         ; Probably doesn't make sense to
                                         ; shrink concurrent histories
                                         :concurrency 1)
                                  jepsen/run!)]
                     (reify Result
                       (pass? [_]
                         (= true (:valid? (:workload (:results test)))))
                       (result-data [_]
                         {:workload (:workload (:results test))
                          :dir (.getCanonicalPath (store/path test))}))))
            ; Great, now run quickcheck
            checked (tc/quick-check 200 prop)]
        ;(pprint checked)
        (println)
        (let [dir (-> checked :shrunk :result-data :dir)
              [name time] (take-last 2 (str/split dir #"/"))
              url (str "http://localhost:8080/files/"
                       (URLEncoder/encode name) "/"
                       (URLEncoder/encode time))]
          (println "Shrunk test:" dir)
          (println (str "             " url)))))}})

(defn -main
  "Handles command line arguments. Can run a test, or a web server for browsing
  results."
  [& args]
  (cli/run!
    (merge (cli/serve-cmd)
           (cli/single-test-cmd {:test-fn shell-test
                                 :opt-spec cli-opts})
           quickcheck-cmd)
    args))
