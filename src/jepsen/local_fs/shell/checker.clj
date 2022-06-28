(ns jepsen.local-fs.shell.checker
  "A checkerfor local filesystem operations where we rely on shelling out to
  various commands."
  (:require [clojure [data :refer [diff]]
                     [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [checker :as checker]
                    [generator :as gen]
                    [nemesis :as nem]
                    [store :as store]
                    [util :as util :refer [pprint-str
                                           timeout]]]
            [knossos.op :as op]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.nio.charset StandardCharsets)))

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

(declare dir)

(defn fs
  "A model filesystem. Has an inode table: a map of inode numbers to files, and
  a directory, which is the root directory map."
  []
  {:next-inode-number 0
   :inodes            (sorted-map)
   :dir               (dir)})

(defn inode
  "Constructs an inode entry. Starts off with a :link-count of 0."
  []
  {:link-count 0
   :data       ""})

(defn dir
  "Constructs a directory model. Contains a map of file/directory names to maps
  representing those files or directories. Directories are:

    {:type  :dir
     :files {\"some-filename\" {:type :link ...}}}"
  []
  {:type :dir
   :files {}})

(defn link
  "Constructs a hard link to an inode number"
  [inode-number]
  {:type  :link
   :inode inode-number})

(defn dir?
  "Is this a model directory?"
  [dir]
  (= :dir (:type dir)))

(defn link?
  "Is this a model of a hardlink?"
  [link]
  (= :link (:type link)))

(def file?
  "Asking if something is a file is the same as asking if it's a hardlink."
  link?)

(defn assert-file
  "Throws {:type ::not-file} when x is not a file. Returns file."
  [file]
  (when-not (file? file)
    (throw+ {:type ::not-file}))
  file)

(defn assert-dir
  "Throws {:type ::not-dir} when x is not a directory. Returns dir."
  [dir]
  (when-not (dir? dir)
    (throw+ {:type ::not-dir}))
  dir)

;; Inode management

(defn next-inode-number
  "Takes a filesystem and returns the next free inode number."
  [fs]
  (:next-inode-number fs))

(defn assoc-inode
  "Adds an inode to the filesystem. Increments the next inode number."
  [fs inode-number inode]
  (assert (not (contains? (:inodes fs) inode-number)))
  (assoc fs
         :next-inode-number (inc (:next-inode-number fs))
         :inodes            (assoc (:inodes fs) inode-number inode)))

(defn dissoc-inode
  "Removes an inode from the filesystem."
  [{:keys [inodes] :as fs} inode-number]
  (assert (= 0 (:link-count (get inodes inode-number))))
  (assoc fs :inodes (dissoc inodes inode-number)))

(defn get-inode
  "Fetches the inode entry in a filesystem."
  [fs inode-number]
  (get-in fs [:inodes inode-number]))

(defn get-link
  "Takes a link in a filesystem and returns the corresponding inode."
  [fs link]
  (get-inode fs (:inode link)))

(defn update-inode
  "Takes a filesystem and an inode number, and a function which transforms that
  inode--e.g. changing its :data field. Returns the filesystem with that update
  applied."
  [fs inode-number f & args]
  (apply update-in fs [:inodes inode-number] f args))

(defn update-inode-link-count
  "Increments (or decrements) the link count for an inode number by `delta`. If
  the inode count falls to zero, deletes the inode."
  [fs inode-number delta]
  (let [inodes (:inodes fs)
        inode  (or (get inodes inode-number)
                   (throw+ {:type         ::no-such-inode
                            :inode-number inode-number
                            :fs           fs}))
        inode' (update inode :link-count + delta)
        inodes' (if (zero? (:link-count inode'))
                  (dissoc inodes inode-number)
                  (assoc inodes inode-number inode'))]
    (assoc fs :inodes inodes')))

(defn inodes-in-entry
  "Finds all inodes (including duplicates!) referred to by all links in a dir
  entry (e.g. a dir or link) recursively."
  [entry]
  (cond (link? entry) [(:inode entry)]
        (dir? entry)  (mapcat inodes-in-entry (vals (:files entry)))
        true          nil))

(defn track-inode-link-count-changes
  "Takes a filesystem and old and new versions of some path--i.e. directories
  or links. Either may be nil, indicating that the path is being created or
  deleted. If the multiset of inodes changes, updates the inode counts in the
  fs inode table."
  [fs entry entry']
  (let [inodes  (frequencies (inodes-in-entry entry))
        inodes' (frequencies (inodes-in-entry entry'))]
    (reduce (fn [fs inode]
              (let [delta (- (get inodes' inode 0)
                             (get inodes  inode 0))]
                (if (zero? delta)
                  fs
                  (update-inode-link-count fs inode delta))))
            fs
            (distinct (concat (keys inodes) (keys inodes'))))))

(defn assert-valid-fs
  "Checks that a filesystem is valid--e.g. that its inodes are not dangling."
  [fs]
  (let [dir-inodes (set (inodes-in-entry (:dir fs)))
        dangling   (remove dir-inodes (keys (:inodes fs)))]
    (when (seq dangling)
      (throw+ {:type     ::dangling-inodes
               :dangling dangling
               :dir-inodes dir-inodes
               :fs       fs})))
  fs)

;; Directory traversal

(defn get-path*
  "Takes a filesystem and a path (as a vector of strings). Returns the entry at
  that position, or nil iff it doesn't exist. The empty path refers to the
  root. Throws:

    {:type ::not-dir}  When some directory in path isn't a directory."
  ([fs path]
   (get-path* fs (:dir fs) path))
  ([fs root path]
   (when root
     (if (seq path)
       (do (assert-dir root)
           (let [[filename & more] path
                 file (get (:files root) filename)]
             (if (seq more)
               ; Descend
               (get-path* fs file more)
               ; We got it
               file)))
       ; We're asking for the root itself
       root))))

(defn get-path
  "Like get-path*, but throws :type ::does-not-exist when the thing doesn't
  exist."
  [fs path]
  (if-let [entry (get-path* fs path)]
    entry
    (throw+ {:type ::does-not-exist})))

(defn assoc-path
  "Takes a filesystem and a path (as a vector of strings) and a new file/dir
  entry. The empty path refers to the root. Returns the filesystem with that
  path set to that file/dir. Assoc'ing nil deletes that path. Also tracks
  changes to link counts.

  Throws:

    {:type ::not-dir}        When some directory in path isn't a directory.
    {:type ::does-not-exist} When some directory in path doesn't exist."
  ([fs path entry']
   ; We need to know both the old and new states of the path so that we can
   ; keep link counts up to date. To keep the recursion simpler, we capture the
   ; current value of the entry in a promise.
   (let [entry-promise (promise)
         root' (assoc-path (:dir fs) path entry-promise entry')]
     (-> (assoc fs :dir root')
         (track-inode-link-count-changes @entry-promise entry'))))
  ([root path entry-promise entry']
   (when-not root
     (throw+ {:type ::does-not-exist}))
   (assert-dir root)
   (if (seq path)
     (let [[filename & more] path
           entry (get (:files root) filename)]
       (if more
         ; Descend
         (assoc-in root [:files filename]
                   (assoc-path entry more entry-promise entry'))
         ; This is the final directory
         (do (deliver entry-promise (get-in root [:files filename]))
             (if (nil? entry')
               ; Delete
               (update root :files dissoc filename)
               ; Update
               (assoc-in root [:files filename] entry')))))
     ; Replace this root
     (do (deliver entry-promise dir)
         entry'))))

(defn update-path*
  "Takes a filesystem and a path (as a vector of strings) and a
  function that transforms whatever file/dir entry is at that path. The empty
  path refers to the root. If the thing referred to by the path doesn't exist,
  passes `nil` to transform. To delete an entry, return nil from transform.

  Throws:

    {:type ::not-dir}        When some directory in path isn't a directory.
    {:type ::does-not-exist} When some directory in path doesn't exist."
  [fs path transform]
  (let [file  (get-path* fs path)
        file' (transform file)]
    (assoc-path fs path file')))

(defn update-dir
  "Takes a filesystem, a path (as a vector of strings), and a function
  which takes a directory map and modifies it somehow. Descends into the given
  path, applies f to it, and returns the modified root directory.

  Throws:

    {:type ::not-dir}        When some path component isn't a directory.
    {:type ::does-not-exist} When the path doesn't exist."
  [fs path transform]
  (update-path* fs path (fn [dir]
                          (when-not dir
                            (throw+ {:type ::does-not-exist}))
                          (assert-dir dir)
                          (transform dir))))

(defn update-file*
  "Takes an fs, a path to a file (a vector of strings), and a function which
  takes that file's inode and modifies it. Descends into that directory,
  applies f to the file's corresponding inode, and returns the new fs.

  This variant allows non-existent files to be modified: transform will receive
  `nil` if the file doesn't exist, but its directory does.

  Throws everything update-dir does, plus

    {:type ::not-file}  If the path exists but isn't a file"
  [fs path transform]
  (if-let [link (get-path* fs path)]
    ; Modifying an existing file.
    (let [inode (get-link fs (assert-file link))]
      (update-inode fs (:inode link) transform))
    ; Creating a new file.
    (let [inode-number (next-inode-number fs)
          inode        (transform nil)]
      (-> fs
          ; Save the new inode
          (assoc-inode inode-number inode)
          ; And create the link to it
          (assoc-path path (link inode-number))))))

(defn update-file
  "Takes a filesystem, a path to a file (a vector of strings), and a function
  which takes that file and modifies it. Descends into that directory, applies
  f to the file's inode, and returns the new filesystem.

  Throws everything update-dir does, plus

  {:type ::does-not-exist}  If the file doesn't exist
  {:type ::not-file}        If the path is not a file"
  [fs path transform]
  (update-file* fs path
                (fn [file]
                  (when-not file
                    (throw+ {:type ::does-not-exist}))
                  (transform file))))

(defn dissoc-path
  "Takes a filesystem and a path (as a vector of strings). Deletes that
  path and returns root. Throws:

    {:type ::not-dir}             When some directory in path isn't a directory
    {:type ::does-not-exist}      When some part of the path doesn't exist
    {:type ::cannot-dissoc-root}  When trying to dissoc the root"
  [fs path]
  (if (seq path)
    (update-path* fs path (fn [entry]
                              (if entry
                                nil
                                (throw+ {:type ::does-not-exist}))))
    (throw+ {:type ::cannot-dissoc-root})))

(defn fs-op
  "Applies a filesystem operation to a simulated in-memory filesystem state.
  Returns a pair of [fs', op']: the resulting state, and the completion
  operation."
  [fs {:keys [f value] :as op}]
  (try+
    (case f
      :append
      (try+
        (let [[path data] value]
          [(update-file* fs path
                         (fn [in]
                           (let [in (or in (inode))]
                             (update in :data str data))))
           (assoc op :type :ok)]))

      :ln
      (let [[from-path to-path] value
            from-entry (assert-file (get-path fs from-path))
            to-entry   (get-path* fs to-path)
            ; If link target is a directory, create a link *inside* that dir
            to-path    (if (dir? to-entry)
                         (vec (concat to-path [(last from-path)]))
                         to-path)
            to-entry   (get-path* fs to-path)
            ; Actually link
            fs' (update-path* fs to-path
                              (fn [link]
                                (when-not (nil? link)
                                  (throw+ {:type ::exists}))
                                from-entry))]
        [fs' (assoc op :type :ok)])

      :mkdir
      [(update-path* fs value
                     (fn [path]
                       ; We expect this to be empty
                       (when-not (nil? path)
                         (throw+ {:type ::exists}))
                       (dir)))
       (assoc op :type :ok)]

      :mv (try+
            (let [[from-path to-path] value
                  from-entry (get-path* fs from-path)
                  to-entry   (get-path* fs to-path)
                  ; If moving to a directory, put us *inside* the given dir
                  to-path    (if (dir? to-entry)
                               (vec (concat to-path [(last from-path)]))
                               to-path)
                  to-entry   (get-path* fs to-path)]

              ; Look at ALL THESE WAYS TO FAIL
              (assert-dir (get-path* fs (parent-dir to-path)))

              (when (nil? from-entry)
                (throw+ {:type ::does-not-exist}))

              (when (or (= from-path to-path)
                        ; These could be two distinct paths with links to the
                        ; same inode
                        (and (link? from-entry) (= from-entry to-entry)))
                (throw+ {:type ::same-file}))

              (when (and (dir? to-entry)
                         (not (dir? from-entry)))
                (throw+ {:type ::cannot-overwrite-dir-with-non-dir}))

              (when (and to-entry
                         (dir? from-entry)
                         (not (dir? to-entry)))
                (throw+ {:type ::cannot-overwrite-non-dir-with-dir}))

              (when (= from-path (take (count from-path) to-path))
                (throw+ {:type ::cannot-move-inside-self}))

              ; When moving a directory on top of another directory, the target
              ; must be empty.
              (when (and (dir? from-entry)
                         (dir? to-entry)
                         (seq (:files to-entry)))
                (throw+ {:type ::not-empty}))

              [(-> fs
                   (assoc-path to-path from-entry)
                   (dissoc-path from-path))
               (assoc op :type :ok)])
            ; Working out the exact order that mv checks and throws errors
            ; has proven incredibly hard so we collapse some of them to
            ; one value.
            (catch [:type ::not-dir] _
              (throw+ {:type ::does-not-exist})))

      :read
      (let [path  (first value)
            entry (assert-file (get-path fs path))
            inode (get-link fs entry)]
        [fs (assoc op :type :ok, :value [path (:data inode)])])

      :rm
      (do (get-path fs value) ; Throws if does not exist
          [(dissoc-path fs value) (assoc op :type :ok)])

      :touch
      [(if (get-path* fs value)
         fs ; Already exists
         (update-file* fs value (fn [_] (inode))))
       (assoc op :type :ok)]

      :truncate
      (let [[path delta] value]
        [(update-file* fs path
                       (fn [extant-inode]
                         (let [inode (or extant-inode (inode))
                               data (.getBytes (:data inode))
                               size  (max 0 (+ (alength data) delta))
                               data' (byte-array size)
                               _     (System/arraycopy
                                       ^bytes data (int 0)
                                       ^bytes data' (int 0) (int size))
                               data' (String. data' StandardCharsets/UTF_8)]
                           (assoc inode :data data'))))
         (assoc op :type :ok)])

      :write
      (let [[path data] value
            fs' (update-file* fs path
                              (fn [extant-inode]
                                (-> (or extant-inode (inode))
                                    (assoc :data data))))]
        [fs' (assoc op :type :ok)]))
    (catch [:type ::cannot-move-inside-self] e
      [fs (assoc op :type :fail, :error :cannot-move-inside-self)])
    (catch [:type ::cannot-overwrite-dir-with-non-dir] e
      [fs (assoc op :type :fail, :error :cannot-overwrite-dir-with-non-dir)])
    (catch [:type ::cannot-overwrite-non-dir-with-dir] e
      [fs (assoc op :type :fail, :error :cannot-overwrite-non-dir-with-dir)])
    (catch [:type ::does-not-exist] e
      [fs (assoc op :type :fail, :error :does-not-exist)])
    (catch [:type ::exists] e
      [fs (assoc op :type :fail, :error :exists)])
    (catch [:type ::not-dir] e
      [fs (assoc op :type :fail, :error :not-dir)])
    (catch [:type ::not-empty] e
      [fs (assoc op :type :fail, :error :not-empty)])
    (catch [:type ::not-file] e
      [fs (assoc op :type :fail, :error :not-file)])
    (catch [:type ::same-file] e
      [fs (assoc op :type :fail, :error :same-file)])))

(defn simple-trace
  "Takes a collection of ops from a history and strips them down to just [type
  f value maybe-error] tuples, to make it easier to read traces from
  fs-checker."
  [history]
  (->> history
       (mapv (fn [{:keys [type f value error]}]
               (if error
                 [type f value error]
                 [type f value])))))

(defn pure-checker
  "A checker which compares the actual history to a simulated model
  filesystem."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [; Simulate evolution
            [fss model-history]
            (loopr [fs       (fs)
                    invoke   nil
                    fss      []
                    history' []]
                   [op history]
                   (cond (= :nemesis (:process op))
                         (recur fs
                                invoke
                                (conj fss fs)
                                (conj history' op))

                         (= :invoke (:type op))
                         (recur fs
                                op
                                (conj fss fs)
                                (conj history' op))

                         true
                         (let [[fs' op'] (fs-op fs invoke)
                               ; Leaving this commented out for speed, but if
                               ; we see weird fs structures, this incremental
                               ; fsck on each op is nice
                               ;_ (try+ (assert-valid-fs fs')
                               ;        (catch clojure.lang.ExceptionInfo e
                               ;          (throw+ {:type ::invalid-fs-state
                               ;                   :fs   fs
                               ;                   :op   invoke})))
                               ; We want to use the index and timing info from
                               ; the actual completion
                               op' (assoc op'
                                          :time (:time op)
                                          :index (:index op))]
                           (recur fs'
                                  nil
                                  (conj fss fs')
                                  (conj history' op'))))
                   [fss history'])
            ;_ (info :history (pprint-str history))
            ;_ (info :model-history (pprint-str model-history))
            ; Look for earliest divergence
            divergence (loop [i 0]
                         (if (<= (count history) i)
                           ; Done!
                           nil
                           (let [actual   (nth history i)
                                 expected (nth model-history i)]
                             (if (= actual expected)
                               ; Fine
                               (recur (inc i))
                               ; Not fine
                               (let [; Compute a window of indices
                                     is (range 0 #_(max 0 (- i 10)) (inc i))
                                     ; And take those recent ops...
                                     ops (->> (map history is)
                                              (remove op/invoke?))
                                     ; And the state just prior to i, and after
                                     ; applying i
                                     fs  (if (pos? i)
                                             (nth fss (dec i))
                                             (fs))
                                     fs' (nth fss i)]
                                 {:index    i
                                  :trace    (simple-trace ops)
                                  :fs       fs
                                  :expected expected
                                  :actual   actual})))))]
        (if divergence
          (assoc divergence
                 :valid? false)
          {:valid? true})))))

(defn print-fs-test
  "Prints out a (presumably failing) fs test to stdout"
  [res]
  (doseq [op (:trace res)]
    (prn op))
  (println)
  (println "At this point, the fs was theoretically")
  (pprint (:fs res))
  (println)
  (println "And we expected to execute")
  (pprint (:expected res))
  (println)
  (println "But with our filesystem we actually executed")
  (pprint (:actual res)))

(defn checker
  "A checker for shell FS operations. Like pure-checker, but also prints out
  files."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [res (checker/check (pure-checker) test history opts)]
        (when-not (:valid? res)
          (store/with-out-file test "fs-test.log"
            (print-fs-test res)))
        res))))
