(ns jepsen.local-fs.shell.client
  "A client for local filesystem operations where we rely on shelling out to
  various commands."
  (:require [clojure [data :refer [diff]]
                     [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java [io :as io]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [client :as client]
                    [util :as util :refer [pprint-str
                                           name+
                                           timeout]]]
            [jepsen.local-fs [util :refer [sh *sh-trace*]]]
            [knossos.op :as op]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.lang Process
                      ProcessBuilder
                      ProcessBuilder$Redirect)
           (java.io File
                    IOException
                    OutputStreamWriter
                    Writer)
           (java.util.concurrent TimeUnit)))

(defn split-path
  "Splits a path string on / separators."
  [path-str]
  (str/split #"/" path-str))

(defn join-path
  "Joins a seqable of strings with / into a single string."
  [path-fragments]
  (str/join "/" path-fragments))

(defn parent-dir
  "Given a path as a vector of strings, returns the parent directory of that
  path. Nil if the path has no directories."
  [path]
  (butlast path))

(defn fs-op!
  "Applies a filesystem operation to the local node."
  [dir {:keys [f value] :as op}]
  (case f
    :append
    (try+ (let [[path data] value]
            (sh :bash :-c (str "cat >> " (join-path path))
                {:dir dir, :in data})
            (assoc op :type :ok))
          (catch [:exit 1] e
            (assoc op
                   :type :fail
                   :error (condp re-find (:err e)
                            #"Is a directory"  :not-file
                            #"Not a directory" :not-dir
                            #"No such file"    :does-not-exist

                            (throw+ e)))))

    :ln
    (let [[from to] value]
      (try+ (sh :ln (join-path from) (join-path to) {:dir dir})
            (assoc op :type :ok)
            (catch [:exit 1] e
              (assoc op
                     :type :fail
                     :error (condp re-find (:err e)
                              #"File exists"               :exists
                              #"Not a directory"           :not-dir
                              #"not allowed for directory" :not-file
                              #"No such file or directory" :does-not-exist
                              (throw+ e))))))


    :mkdir
    (try+ (sh :mkdir (join-path value) {:dir dir})
          (assoc op :type :ok)
          (catch [:exit 1] e
            (assoc op
                   :type :fail
                   :error (condp re-find (:err e)
                            #"File exists"               :exists
                            #"Not a directory"           :not-dir
                            #"No such file or directory" :does-not-exist
                            (throw+ e)))))

    :mv
    (try+ (let [[from to] value]
            (sh :mv (join-path from) (join-path to) {:dir dir})
            (assoc op :type :ok))
          (catch [:exit 1] e
            (assoc op
                   :type :fail
                   :error (condp re-find (:err e)
                            #"are the same file"         :same-file
                            #"cannot move .+ to a subdirectory of itself"
                            :cannot-move-inside-self
                            #"cannot overwrite directory .+ with non-directory"
                            :cannot-overwrite-dir-with-non-dir
                            #"cannot overwrite non-directory .+ with directory"
                            :cannot-overwrite-non-dir-with-dir
                            #"Directory not empty"
                            :not-empty
                            #"Not a directory"           ;:not-dir
                            :does-not-exist ; We collapse these for convenience
                            #"No such file or directory" :does-not-exist

                            (throw+ e)))))

    :read
    (try+ (let [[path] value
                data   (sh :cat (join-path path) {:dir dir})]
            (assoc op :type :ok, :value [path data]))
          (catch [:exit 1] e
            (assoc op
                   :type :fail
                   :error (condp re-find (:err e)
                            #"Is a directory"            :not-file
                            #"Not a directory"           :not-dir
                            #"No such file or directory" :does-not-exist

                            (throw+ e)))))

    :rm (try+ (do (sh :rm :-r (join-path value) {:dir dir})
                  (assoc op :type :ok))
              (catch [:exit 1] e
                (assoc op
                       :type :fail
                       :error (condp re-find (:err e)
                                #"Not a directory"           :not-dir
                                #"No such file or directory" :does-not-exist
                                (throw+ e)))))

    :touch
    (try+ (sh :touch (join-path value) {:dir dir})
          (assoc op :type :ok)
          (catch [:exit 1] e
            (assoc op
                   :type :fail
                   :error (condp re-find (:err e)
                            #"Not a directory"           :not-dir
                            #"No such file or directory" :does-not-exist
                            (throw+ e)))))

    :truncate
    (let [[path delta] value]
      (try+ (sh :truncate :-s (if (neg? delta)
                                delta
                                (str "+" delta))
                (join-path path)
                {:dir dir})
            (assoc op :type :ok)
            (catch [:exit 1] e
              (info :err (:err e))
              (assoc op
                     :type :fail
                     :error (condp re-find (:err e)
                              #"Not a directory"           :not-dir
                              #"No such file or directory" :does-not-exist
                              #"Is a directory"            :not-file
                              (throw+ e))))))

    :write (let [[path data] value]
             (try+ (sh :bash :-c (str "cat > " (join-path path))
                       {:dir dir, :in data})
                   (assoc op :type :ok)
                   (catch [:exit 1] e
                     (assoc op
                            :type :fail
                            :error (condp re-find (:err e)
                                     #"Is a directory"  :not-file
                                     #"Not a directory" :not-dir
                                     #"No such file"    :does-not-exist

                                     (throw+ e))))))))

(defrecord FSClient [dir]
  client/Client
  (open! [this test node]
    this)

  (setup! [this test]
    this)

  (invoke! [this test op]
    (timeout 10000 (assoc op :type :info, :error :timeout)
             (fs-op! dir op)))

  (teardown! [this test])

  (close! [this test]))

(defn client
  "Constructs a client which performs filesystem ops in the given directory."
  [dir]
  (map->FSClient {:dir dir}))
