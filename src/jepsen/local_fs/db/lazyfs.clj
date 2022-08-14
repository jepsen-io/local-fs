(ns jepsen.local-fs.db.lazyfs
  "A DB which mounts the given directory as a lazyfs filesystem."
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [db :as db]
                    [lazyfs :as lazyfs]
                    [util :as util :refer [await-fn meh timeout]]]
            [jepsen.local-fs [util :refer [sh sh* *sh-trace*]]]
            [jepsen.local-fs.db.core :refer [LoseUnfsyncedWrites]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def current-version
  "What version do we have currently built?"
  (atom nil))

(def lazyfs-dir
  "Where do we compile lazyfs?"
  "lazyfs")

(def bin
  "Where does the lazyfs binary live?"
  (str lazyfs-dir "/lazyfs/build/lazyfs"))

(defn install!
  "Installs lazyfs locally"
  [version]
  (when-not (= version @current-version)
    (info "Installing lazyfs" version)
    ; Get repo
    (try+ (sh* :ls lazyfs-dir)
          (catch [:exit 2] _
            ; Doesn't exist yet
            (sh :mkdir :-p lazyfs-dir)
            (sh :rmdir lazyfs-dir)
            (sh :git :clone lazyfs/repo-url lazyfs-dir)))
    ; Check out version
    (sh :git :fetch             {:dir lazyfs-dir})
    (sh :git :checkout version  {:dir lazyfs-dir})
    (sh :git :clean :-fx        {:dir lazyfs-dir})
    (sh "./build.sh"            {:dir (str lazyfs-dir "/libs/libpcache")})
    (sh "./build.sh"            {:dir (str lazyfs-dir "/lazyfs")})
    (reset! current-version version)))

(defn start-daemon!
  "Starts the lazyfs daemon. Takes a lazyfs map."
  [{:keys [dir lazyfs-dir data-dir pid-file log-file fifo config-file]}]
  (sh :bash :-c
      (str
        "/sbin/start-stop-daemon"
        " --start"
        " --chdir ."
        " --startas " bin
        " --background"
        " --no-close"
        " --make-pidfile"
        " --pidfile " pid-file
        " --exec " bin
        " --startas " bin
        " --"
        " " dir
        " --config-path " config-file
        " -o allow_other"
        " -o modules=subdir"
        " -o subdir=" data-dir
        ; " -s" ; singlethreaded
        " -f" ; foreground
        " >> "log-file " 2>&1")))

(defn mount!
  "Takes a lazyfs map, creates directories and config files, and starts the
  lazyfs daemon. Returns the lazyfs map."
  [{:keys [dir data-dir lazyfs-dir chown user config-file log-file] :as lazyfs}]
  (info "Mounting lazyfs" dir)
  ; Make directories
  (sh :mkdir :-p dir)
  (sh :mkdir :-p data-dir)
  ; Write config
  (spit config-file (lazyfs/config lazyfs))
  ; And go!
  (start-daemon! lazyfs)
  ; Wait for mount
  (await-fn (fn check-mounted []
              (or (re-find #"lazyfs" (sh :findmnt dir))
                  (throw+ {:type ::not-mounted
                           :dir  dir
                           :log  (slurp log-file)})))
            {:retry-interval 100
             :log-interval 5000
             :log-message "Waiting for lazyfs to mount"})
  lazyfs)

(declare lose-unfsynced-writes!)

(defn umount!
  "Stops the given lazyfs map and destroys the lazyfs directories."
  [{:keys [lazyfs-dir dir] :as lazyfs}]
  (try+
    (info "Unmounting lazyfs" dir)
    ; Speeds up umount
    (meh (lose-unfsynced-writes! lazyfs))
    (sh :fusermount :-uz dir)
    (info "Unmounted lazyfs" dir)
    (catch [:exit 1] _
      ; No such directory
      nil)
    (catch [:exit 127] _
      ; Command not found
      nil))
  (sh :rm :-rf dir lazyfs-dir))

(defn fifo!
  "Sends a string to the fifo channel for the given lazyfs map."
  [{:keys [fifo]} cmd]
  (timeout 1000 (throw+ {:type ::fifo-timeout
                         :cmd  cmd
                         :fifo fifo})
           (spit fifo (str cmd "\n"))))

(defrecord DB [version lazyfs]
  db/DB
  (setup! [this test node]
    (let [version (or version lazyfs/commit)]
      (install! version)
      (mount! lazyfs)))

  (teardown! [this test node]
    (umount! lazyfs)
    )

  LoseUnfsyncedWrites
  (lose-unfsynced-writes! [this]
    (lose-unfsynced-writes! lazyfs)))

(defn db
  "Constructs a new Jepsen DB backed by a local directory, passed in {:dir
  opts}."
  [{:keys [dir version]}]
  ; Use a full path; not everything works with relative
  (let [dir (.getCanonicalPath (io/file dir))]
    (map->DB {:version version
              :lazyfs  (lazyfs/lazyfs dir)})))

(defn lose-unfsynced-writes!
  "Takes a lazyfs map or a DB, and asks the fifo to lose any writes which
  haven't been fsynced yet."
  [db-or-lazyfs-map]
  (if (instance? DB db-or-lazyfs-map)
    (recur (:lazyfs db-or-lazyfs-map))
    (do (info "Losing un-fsynced writes to" (:dir db-or-lazyfs-map))
        (fifo! db-or-lazyfs-map "lazyfs::clear-cache")
        :done)))

(defn checkpoint!
  "Forces the given lazyfs map or DB to flush writes to disk."
  [db-or-lazyfs-map]
  (if (instance? DB db-or-lazyfs-map)
    (recur (:lazyfs db-or-lazyfs-map))
    (do (info "Checkpointing all writes to" (:dir db-or-lazyfs-map))
        (fifo! db-or-lazyfs-map "lazyfs::cache-checkpoint")
        :done)))
