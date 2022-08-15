(defproject jepsen.local-fs "0.1.0-SNAPSHOT"
  :description "Jepsen tests for a local filesystem"
  :url "https://github.com/jepsen-io/local-fs"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.2.7"]
                 [org.clojure/test.check "1.1.1"]
                 [org.clojure/data.avl "0.1.0"]]
  :repl-options {:init-ns jepsen.local-fs}
  :main jepsen.local-fs
  :test-selectors {:default (fn [m]
                              (not (:perf m)))
                   :perf        :perf
                   :focus       :focus})
