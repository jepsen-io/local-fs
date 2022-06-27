(ns jepsen.local-fs
  "Main entry point for local FS testing. Handles CLI arguments, launches
  tests."
  (:require [clojure.tools.logging :refer [info warn]]
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
            [clojure.test.check [properties :as prop]
                                [results :refer [Result]]]))

(def dbs
  "A map of DB names to functions that take CLI options and build DBs."
  {:dir    db.dir/db
   :lazyfs db.lazyfs/db})

(defn shell-test
  "Takes CLI options and constructs a test for the shell workload."
  [opts]
  (let [workload (shell/workload opts)
        db       ((dbs (:db opts)) opts)]
    (merge tests/noop-test
           opts
           {:name   "shell"
            :ssh    {:dummy? true}
            :nodes  ["local"]
            :db      db
            :checker (checker/compose
                       {:perf (checker/perf)
                        :stats (checker/stats)
                        :exceptions (checker/unhandled-exceptions)
                        :workload (:checker workload)})
            :client         (:client workload)})))

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
                         (:valid? (:results test)))
                       (result-data [_]
                         {:dir (.getName (store/path test))}))))]
        ; Great, now run quickcheck
        (tc/quick-check 10 prop)))}})

(defn -main
  "Handles command line arguments. Can run a test, or a web server for browsing
  results."
  [& args]
  (cli/run!
    (merge (cli/serve-cmd)
           quickcheck-cmd)
    args))
