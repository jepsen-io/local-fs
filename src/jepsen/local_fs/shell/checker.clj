(ns jepsen.local-fs.shell.checker
  "A checker for local filesystem operations where we rely on shelling out to
  various commands."
  (:require [clojure [data :refer [diff]]
                     [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.data.avl :as avl]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [checker :as checker]
                    [generator :as gen]
                    [nemesis :as nem]
                    [store :as store]
                    [util :as util :refer [pprint-str
                                           timeout]]]
            [jepsen.local-fs.util :refer [bytes->hex
                                          hex->bytes]]
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

(defn child-path?
  "Is the given path a child of the parent?"
  [parent child]
  (let [parent-len (count parent)]
    (and (< parent-len (count child))
         (= parent (subvec child 0 parent-len)))))

(defn direct-child-path?
  "Is the given path a direct child of the parent?"
  [parent child]
  (and (= (inc (count parent)) (count child))
       (child-path? parent child)))

(defn path-compare
  "Sorts paths alphabetically by component."
  [a b]
  (let [ac   (count a)
        bc   (count b)
        imax (min ac bc)]
    (loop [i 0]
      (if (<= imax i)
        ; Equal prefixes; pick the longer.
        (compare ac bc)
        ; Compare this index
        (let [a_i (nth a i)
              b_i (nth b i)]
          (if (= a_i b_i)
            (recur (inc i))
            (compare a_i b_i)))))))

(declare dir)

(defn fs
  "A model filesystem. Contains a next-inode-number, which is how we allocate
  new inodes. Then there are two states: :disk and :cache.

    {:next-inode-number 0
     :disk {...}
     :cache {...}}

  Each of the :disk and :cache states is a map like:

     :inodes  A map of inode numbers to files, with their data and reference
              counts
     :entries A sorted AVL map of paths (e.g. [:a :b]) to entries
              (a link, dir, or tombstone map)."
  []
  (let [state {:inodes  (avl/sorted-map)
               :entries (avl/sorted-map-by path-compare)}]
    {:next-inode-number 0
     :cache             state
     ; Start with a root directory on disk.
     :disk              (update state :entries assoc [] (dir))}))

(defn inode
  "Constructs an inode entry. Starts off with a :link-count of 0. We represent
  inode data as hex strings, for convenience."
  []
  {:link-count 0
   :data       ""})

(defn dir
  "Constructs a directory entry.

    {:type  :dir
     :files :{\"some-filename\" {:type :link ...}}}"
  []
  {:type :dir})

(defn link
  "Constructs a hard link to an inode number"
  [inode-number]
  {:type  :link
   :inode inode-number})

(def tombstone
  "We use this to represent something deleted from the cache--so we can
  distinguish it from the mere absence of a cached value."
  {:type :tombstone})

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

(defn tombstone?
  "Is this entry a tombstone?"
  [tombstone]
  (= :tombstone (:type tombstone)))

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

(defn assert-exists
  "Throws {:type ::does-not-exist} when the given directory entry is either nil
  or a tombstone. Otherwise returns entry."
  [entry]
  (when (or (nil? entry) (tombstone? entry))
    (throw+ {:type ::does-not-exist}))
  entry)

;; Working with entry maps

(defn children
  "Takes an entry map and a path, presumably referring to a directory. Returns
  a sequence of [path entry] kv pairs which are direct or recursive children of
  the given path."
  [entries path]
  (->> (avl/subrange entries > path)
       (take-while (fn [pair]
                     (child-path? path (key pair))))))

(defn direct-children
  "Takes an entry map and a path, presumably referring to a directory. Returns
  a sequence of [path entry] kv pairs which are direct children (e.g. not
  recursive) of the path."
  [entries path]
  (->> (children entries path)
       (filter (partial direct-child-path? path))))

(defn dissoc-children
  "Takes an entry map and a path; delete that path's children recursively."
  [entries path]
  (->> (children entries path)
       (map key)
       (reduce dissoc! (transient entries))
       (persistent!)))

;; Fsync

(defn fsync-inode
  "Takes a filesystem and fsyncs a specific inode. We do this by moving the
  inode from cache to disk. If the inode has a zero link count, we delete it
  from both disk and cache."
  [fs inode-number]
  (if-let [inode (get-in fs [:cache :inodes inode-number])]
    (if (zero? (:link-count inode))
      ; Sync deletion
      (-> fs
          (update-in [:disk :inodes] dissoc inode-number)
          (update-in [:cache :inodes] dissoc inode-number))
      ; Sync data
      (-> fs
          (assoc-in [:disk :inodes inode-number] inode)
          (update-in [:cache :inodes] dissoc inode-number)))
    ; Nothing to sync
    fs))

(declare fsync-entry)

(defn merge-disk-cache-entries
  "Merges the disk and cache versions of an entry together recursively,
  returning the new disk entry. Used in fsync-root."
  [disk-entry cache-entry]
  ;(prn :merge disk-entry cache-entry)
  (case (:type cache-entry)
    ; For directories, zip through each filename in the cache, recursively
    ; merging into disk.
    :dir
    (reduce (fn [entry [filename child-cache-entry]]
              (let [child-disk-entry (get (:files disk-entry) filename)
                    child-entry'     (merge-disk-cache-entries
                                       child-disk-entry
                                       child-cache-entry)]
                (if child-entry'
                  (update entry :files assoc filename child-entry')
                  ; If nil, it's been deleted.
                  (update entry :files dissoc filename))))
            disk-entry
            (:files cache-entry))

    :link
    cache-entry

    :tombstone
    nil

    nil
    disk-entry))

(defn fsync-path*
  "Fsyncs the entry at a path non-recursively. Does not fsync inodes."
  [fs path]
  (let [disk-entries  (-> fs :disk :entries)
        disked        (get disk-entries path)
        ;_             (prn :fs)
        ;_             (pprint fs)
        ;_             (prn :path path)
        cached        (get-in fs [:cache :entries path])
        ;_ (prn :cached cached)
        ; Remove entry from cache
        fs'           (update-in fs [:cache :entries] dissoc path)]
    (case (:type cached)
      (:dir, :link) (assoc-in fs' [:disk :entries path] cached)

      :tombstone (update-in fs' [:disk :entries] dissoc path))))

(defn fsync-path-recursively*
  "Takes a filesystem and a path. Fsyncs the entry at that path and all its
  children. Does not fsync inodes. With no path, fsyncs the root--i.e. every
  entry."
  ([fs]
   (fsync-path-recursively* fs []))
  ([fs path]
   (-> fs :cache :entries
       (children path)
       (->> (map key)
            (reduce fsync-path* fs)))))

(defn inodes-in-disk-entries
  "Returns a collection (including duplicates) of every inode referenced on
  disk in a filesystem."
  [fs]
  (->> fs :disk :entries vals (keep :inode)))

(defn recreate-missing-inodes
  "Rewrites a filesystem's disk to re-create inodes which are referenced but no
  longer exist."
  [fs]
  (loopr [inodes (transient (:inodes (:disk fs)))]
         [[path entry] (:entries (:disk fs))]
         (if-let [inode-number (:inode entry)]
           (if (get inodes inode-number)
             ; Got it!
             (recur inodes)
             ; Whoops, it's gone; replace it with an empty one.
             (recur (assoc! inodes inode-number (inode))))
           ; No inode at this entry
           (recur inodes))
         (assoc-in fs [:disk :inodes] (persistent! inodes))))

(defn lose-unfsynced-writes
  "Simulates the loss of the cache and recovery of the filesystem purely from
  disk.

  Inodes which are referenced but are not on disk are replaced with empty
  inodes.

  Note that the link counts on disk may not actually be reflective of the links
  themselves--we may have synced an inode but not a directory, for instance, in
  which case the inode would have a higher link count but the directory
  wouldn't point to it. We therefore traverse the actual directory structure on
  disk and update inode link counters from there, deleting inodes if they're
  unreachable."
  [fs-]
  (let [fs' (recreate-missing-inodes fs-)
        ; Build a map of inode numbers to link counts based on the directory
        ; structure.
        link-counts (frequencies (inodes-in-disk-entries fs'))
        inodes' (->> (:inodes (:disk fs'))
                     (reduce (fn [inodes [number inode]]
                               (let [link-count (get link-counts number 0)]
                                 (if (zero? link-count)
                                   ; Unreachable
                                   inodes
                                   ; Fixup link count
                                   (assoc! inodes number
                                           (assoc inode :link-count link-count)))))
                             (transient {}))
                     persistent!)]
    ; Get a fresh cache, and a fixed-up inode map
    (assoc fs'
           :cache (:cache (fs))
           :disk  (assoc (:disk fs') :inodes inodes'))))

;; Inode management

(defn next-inode-number
  "Takes a filesystem and returns the next free inode number."
  [fs]
  (:next-inode-number fs))

(defn assoc-inode
  "Adds an inode to the filesystem cache. Increments the next inode number."
  [fs inode-number inode]
  (assert (not (or (contains? (:inodes (:disk fs)) inode-number)
                   (contains? (:inodes (:cache fs)) inode-number))))
  (-> fs
      (assoc :next-inode-number (inc (:next-inode-number fs)))
      (assoc-in [:cache :inodes inode-number] inode)))

(defn get-inode
  "Fetches the inode entry in a filesystem: first cache, then disk."
  [fs inode-number]
  (or (get-in fs [:cache :inodes inode-number])
      (get-in fs [:disk :inodes inode-number])))

(defn get-link
  "Takes a link in a filesystem and returns the corresponding inode. If no
  inode exists, returns nil."
  [fs link]
  (get-inode fs (:inode link)))

(defn update-inode
  "Takes a filesystem and an inode number, and a function which transforms that
  inode--e.g. changing its :data field. Returns the filesystem with that update
  applied to the cache."
  [fs inode-number f & args]
  (let [inode  (get-inode fs inode-number)
        inode' (apply f inode args)]
    (assoc-in fs [:cache :inodes inode-number] inode')))

(defn update-inode-link-count
  "Increments (or decrements) the cache's link count for an inode number by
  `delta`. If the optional strict? flag is set, throws when no such inode
  exists; otherwise ignores missing inodes."
  ([fs inode-number delta]
   (update-inode-link-count fs inode-number delta false))
  ([fs inode-number delta strict?]
   (if-let [inode (get-inode fs inode-number)]
     (let [inode' (update inode :link-count + delta)]
       (assoc-in fs [:cache :inodes inode-number] inode'))
     ; No such inode
     (if strict?
       (throw+ {:type         ::no-such-inode
                :inode-number inode-number
                :fs           fs})
       fs))))

(defn inodes-at-path
  "Takes a directory entries map and a path in it. Finds all inode numbers
  (including duplicates!) referred to by this specific path, not including
  children."
  [entries path]
  (let [entry (get entries path)]
    (cond (link? entry) [(:inode entry)]
          true          nil)))

(defn inodes-in-path
  "Takes a directory entries map and a path in it. Returns the set of inodes in
  the given path or any of its children."
  [entries path]
  (concat (inodes-at-path entries path)
          (mapcat (partial inodes-at-path entries) (children entries path))))

(defn assert-valid-fs
  "Checks that a filesystem is valid--e.g. that its inodes are not dangling."
  [fs]
  ; Haven't reimplemented for new cache/disk fs
  ;(let [inodes     (set (inodes-in-path (:entries (:disk fs))))
  ;      dangling   (remove dir-inodes (keys (:inodes fs)))]
  ;  (when (seq dangling)
  ;    (throw+ {:type     ::dangling-inodes
  ;             :dangling dangling
  ;             :dir-inodes dir-inodes
  ;             :fs       fs})))
  ;fs
  )

;; Directory traversal

(defn child-paths
  "Takes a filesystem and a path (as a vector of strings). Returns a sequence
  of paths for all children of that path in the filesystem."
  [fs path]
  ; Take all the disk entries
  (let [disk-paths (->> (children (:entries (:disk fs)) path)
                        (map key)
                        (into (avl/sorted-set-by path-compare)))]
    ; And merge in the cache
    (persistent!
      (reduce (fn [paths [path cache-entry]]
                (if (tombstone? cache-entry)
                  (dissoc! paths path)
                  (conj! paths path)))
              (transient disk-paths)
              (-> fs :cache :entries (children path))))))

(declare get-path)

(defn assert-path-parent
  "Ensures that the parent directory of a path exists and is actually a
  directory. Throws {:type ::does-not-exist} if parent doesn't exist, {:type
  ::not-dir} if the parent isn't a directory."
  [fs path]
  (if (<= (count path) 1)
    ; Root always exists
    path
    (let [;_ (prn :assert-path-parent path (parent-dir path))
          entry (get-path fs (parent-dir path))]
      ;(prn :entry entry)
      (assert-dir entry)
      ;(prn :parent path :is-dir)
      path)))

(defn get-path*
  "Takes a filesystem and a path (as a vector of strings). Returns the entry at
  that position, or nil iff it doesn't exist. The empty path refers to the
  root. Throws {:type ::not-dir} if the parent of this path is not a
  directory."
  [fs path]
  ; Try to look up the path in the cache, then fall back to disk.
  (let [cached (get-in fs [:cache :entries path])]
    (cond (nil? cached)
          (let [disk (get-in fs [:disk :entries path])]
            ; If it's nil on disk, we should double-check that the parent is
            ; actually a directory.
            (when-not disk (assert-path-parent fs path))
            disk)

          ; If we have a tombstone cached, we also need to check that the
          ; parent is a path; it might *also* be a tombstone.
          (tombstone? cached)
          (do (assert-path-parent fs path)
              nil)

          ; Othewise return the cached entry
          true cached)))

(defn get-path
  "Like get-path*, but throws :type ::does-not-exist when the thing doesn't
  exist."
  [fs path]
  (assert-exists (get-path* fs path)))

(defn assoc-path*
  "Helper for assoc-path which only modifies the path itself, and does not
  recurse into subdirectories. Returns [fs' entry]."
  [fs path entry']
  ;(prn :assoc-path* :path path :entry' entry')
  (let [; What entries do we have currently at this path?
        disk-entry  (get-in fs [:disk :entries path])
        cache-entry (get-in fs [:cache :entries path])
        entry       (or cache-entry disk-entry)
        ; If we're writing nil (deleting), we need to actually create a
        ; tombstone entry.
        entry'      (or entry' tombstone)
        ;_ (prn :disk-entry disk-entry :cache cache-entry :entry entry :entry' entry')
        ; Update the cache for this path
        fs'         (assoc-in fs [:cache :entries path] entry')
        ; Did the inode count change?
        inode       (:inode entry)
        inode'      (:inode entry')
        fs' (if (= inode inode')
              ; No change to inodes
              fs'
              ; Increment/decrement inode refcounts
              (let [fs' (if (nil? inode)
                          fs'
                          (update-inode-link-count fs' inode -1))
                    fs' (if (nil? inode')
                          fs'
                          (update-inode-link-count fs' inode' 1))]
                fs'))]
    [fs' entry]))

(defn assoc-path
  "Takes a filesystem and a path (as a vector of strings) and a new file/dir
  entry. The empty path refers to the root. Returns the filesystem with that
  path set to that file/dir. Assoc'ing nil deletes that path. Also tracks
  changes to link counts.

  Throws:

    {:type ::not-dir}        When some directory in path isn't a directory.
    {:type ::does-not-exist} When some directory in path doesn't exist."
  [fs path entry']
  ;(prn :assoc-path path entry')
  (assert-path-parent fs path)
  ;(prn :parent-ok)
  (let [; Update this particular path
        [fs' entry] (assoc-path* fs path entry')]
    (if (and (dir? entry)
             (not (dir? entry')))
      ; If we changed a directory to something that *wasn't* a directory, we
      ; also need to assoc tombstones for every path (either in cache or disk)
      ; that was under that directory.
      (reduce (fn [fs' path]
                (first (assoc-path* fs' path tombstone)))
              fs'
              (child-paths fs path))
      ; No need to update children
      fs')))

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
                          (assert-exists dir)
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
  ;(prn :update-file* path)
  (if-let [link (get-path* fs path)]
    ; Modifying an existing file.
    (do (assert-file link)
        (update-inode fs (:inode link) transform))
    ; Creating a new file.
    (let [;_            (prn :new-file)
          inode-number (next-inode-number fs)
          inode        (transform nil)]
      ;(prn :inode-number inode-number :inode inode)
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
                  (assert-exists file)
                  (transform file))))

(defn dissoc-path
  "Takes a filesystem and a path (as a vector of strings). Deletes that
  path and returns root. Throws:

    {:type ::not-dir}             When some directory in path isn't a directory
    {:type ::does-not-exist}      When some part of the path doesn't exist
    {:type ::cannot-dissoc-root}  When trying to dissoc the root"
  [fs path]
  ;(prn :dissoc-path path)
  (if (seq path)
    (update-path* fs path (fn [entry]
                            (assert-exists entry)
                            nil))
    (throw+ {:type ::cannot-dissoc-root})))

(defn fs-op*
  "A helper for fs-op which doesn't fsync the root."
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

      :fsync
      (let [entry (get-path fs value)]
        ;(prn :entry entry)
        [(cond (link? entry) (fsync-inode fs (:inode entry))

               ; Right now directories are synced automatically.
               (dir? entry) fs

               :else (throw+ {:type  ::can't-fsync
                              :path  value
                              :entry entry}))
         (assoc op :type :ok)])

      :ln
      (let [[from-path to-path] value
            ;_ (prn :ln)
            from-entry (assert-file (get-path fs from-path))
            ;_ (prn :from-entry from-entry)
            to-entry   (get-path* fs to-path)
            ;_ (prn :to-entry to-entry)
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

      :lose-unfsynced-writes
      [(lose-unfsynced-writes fs)
       (assoc op :type :ok)]

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
              ;(prn :mv)

              ; Look at ALL THESE WAYS TO FAIL
              (assert-dir (get-path* fs (parent-dir to-path)))
              ;(prn :parent-is-dir)
              (assert-exists from-entry)
              ;(prn :from-exists)

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
                         (seq (child-paths fs to-path)))
                (throw+ {:type ::not-empty}))

              ; Actually do the move. First, find the relative paths of all
              ; children
              (let [children (->> (child-paths fs from-path)
                                  (map (fn relative [path]
                                         (subvec path (count from-path)))))
                    ; Destroy the path itself--this will recursively delete the
                    ; children.
                    fs' (dissoc-path fs from-path)
                    ; And copy the path to its new location
                    fs' (assoc-path fs' to-path from-entry)
                    ; Now copy the children to their new home
                    fs' (reduce (fn copy-child [fs' child-path]
                                  (let [from-path (into from-path child-path)
                                        to-path   (into to-path child-path)
                                        entry     (get-path* fs from-path)]
                                    (assoc-path fs' to-path entry)))
                                fs'
                                children)]
              [fs' (assoc op :type :ok)]))
            ; Working out the exact order that mv checks and throws errors
            ; has proven incredibly hard so we collapse some of them to
            ; one value.
            (catch [:type ::not-dir] _
              (throw+ {:type ::does-not-exist})))

      :read
      (let [path  (first value)
            entry (assert-file (get-path fs path))
            ; If the inode is missing, we yield an empty file.
            inode (or (get-link fs entry)
                      (inode))]
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
                               data  (-> inode :data hex->bytes)
                               size  (max 0 (+ (alength data) delta))
                               data' (byte-array size)
                               _     (System/arraycopy
                                       ^bytes data  0
                                       ^bytes data' 0 (min size (alength data)))
                               data' (bytes->hex data')]
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

(defn fs-op
  "Applies a filesystem operation to a simulated in-memory filesystem state.
  Returns a pair of [fs', op']: the resulting state, and the completion
  operation.

  Every operation fsyncs the root metadata--though not the data. This is how
  lazyfs works right now--it writes metadata directly through and doesn't cache
  it."
  [fs op]
  (let [[fs' op'] (fs-op* fs op)]
    [(fsync-path-recursively* fs') op']))

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
