(ns jepsen.local-fs.db.dir
  "A simple DB backed by a local directory--great for testing whatever
  filesystem you're already using."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [db :as db]]
            [jepsen.local-fs [util :refer [sh]]]))

(defn db
  "Constructs a new Jepsen DB backed by a local directory, passed in {:dir
  opts}."
  [{:keys [dir]}]
  (reify db/DB
    (setup! [this test node]
      (info "Making directory" dir)
      (sh :mkdir :-p dir))

    (teardown! [this test node]
      (sh :bash :-c (str "rm -rf " dir "/* " dir "/.*")))))
