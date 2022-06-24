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
            [jepsen.local-fs.db [dir :as db.dir]]
            [clojure.test.check :as tc]
            [clojure.test.check [properties :as prop]
                                [results :refer [Result]]]))

(defn shell-test
  "Takes CLI options and constructs a test for the shell workload."
  [opts]
  (let [workload (shell/workload opts)
        db       (db.dir/db opts)]
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
  [["-d" "--dir DIR" "What directory should we use for our mount point?"
    :default "data"]])

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
