(ns jepsen.local-fs.shell.checker-test
  "Some basic example-based tests for the checker--faster to run these than to
  compare against a full filesystem, and easier to understand when the basics
  go wrong."
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [dom-top.core :refer [loopr]]
            [jepsen.local-fs.shell.checker :as c]))

(defn apply-op
  "Applies an [f value] fs op to a filesystem, returning [fs' [f value']], or
  [fs' [f value' error] if an error occurs."
  [fs [f value]]
  (let [[fs' {:keys [f value error] :as op'}] (c/fs-op fs {:f f :value value})]
    [fs' (if error
           [f value error]
           [f value])]))

(defn apply-ops
  "Applies a sequence of [f value] ops to a filesystem, returning [fs' ops']."
  [fs ops]
  (loopr [fs   fs
          ops' (transient [])]
         [op ops]
         (do (prn)
             (prn :op op)
             (pprint fs)
             (let [[fs' op'] (apply-op fs op)]
               (prn :op' op')
               (recur fs' (conj! ops' op'))))
         [fs (persistent! ops')]))

(defn run
  "Applies a sequence of [f value] ops to a filesystem, returning ops'."
  [fs ops]
  (second (apply-ops fs ops)))

(defn invocation
  "Takes an [f value ...] completion op and returns the invocation that should
  have produced it."
  [[f value]]
  [f (case f
       ; For reads, value is [path data]; we replace data with nil.
       :read (assoc value 1 nil)
       value)])

(defn expect
  "Takes a series of [f value & error] ops, and asserts that running the
  corresponding invocations against those ops produces those same
  values/errors."
  [ops]
  (is (= ops
         (run (c/fs)
              (mapv invocation ops)))))

; Touches a file and reads it back, then deletes it.
(deftest touch-delete
  (is (= [[:touch [:a]]
          [:read  [[:a] ""]]
          [:rm    [:a]]
          [:read  [[:a] nil] :does-not-exist]]
         (run (c/fs)
              [[:touch [:a]]
               [:read  [[:a] nil]]
               [:rm    [:a]]
               [:read  [[:a] nil]]
               ]))))

; Writes a file, crashes, then reads
(deftest write-crash
  (is (= [[:write [["b"] "00"]]
          [:lose-unfsynced-writes nil]
          ; Note that we sync metadata immediately, so the file exists now,
          ; it's just empty
          [:read  [["b"] ""]]]
         (run (c/fs)
              [[:write [["b"] "00"]]
               [:lose-unfsynced-writes nil]
               [:read [["b"] nil]]]))))

; Writes a file, fsyncs it, crashes; it should still be there.
(deftest write-fsync-crash
  (is (= [[:write [[:a] "1a"]]
          [:read [[:a] "1a"]]
          [:fsync [:a]]
          [:read [[:a] "1a"]]
          [:lose-unfsynced-writes nil]
          [:read [[:a] "1a"]]]
         (run (c/fs)
              [[:write [[:a] "1a"]]
               [:read [[:a] nil]]
               [:fsync [:a]]
               [:read [[:a] nil]]
               [:lose-unfsynced-writes nil]
               [:read [[:a] nil]]]
              ))))

; Sort of a weird edge case: the error we expect to get when linking a/a -> a
; should be :not-dir, rather than :does-not-exist.
(deftest ln-child-of-file-into-self
  (is (= [[:touch [:a]]
          [:ln [[:a :a] [:a]] :not-dir]]
         (run (c/fs)
              [[:touch [:a]]
               [:ln [[:a :a] [:a]]]]))))

; Move a file
(deftest create-mv
  (is (= [[:append [[:b] "a1"]]
          [:mv [[:b] [:a]]]
          [:read [[:a] "a1"]]]
         (run (c/fs)
              [[:append [[:b] "a1"]]
               [:mv [[:b] [:a]]]
               [:read [[:a] nil]]]))))

; Move one directory onto another which isn't empty
(deftest mv-dir-onto-nonempty-dir
  (is (= [[:mkdir ["a"]]
          [:mkdir ["a" "b"]]
          [:truncate [["b"] 0]]
          [:mv [["b"] ["a" "b"]]]
          [:mkdir ["b"]]
          [:mv [["b"] ["a"]] :not-empty]]
         (run (c/fs)
              [[:mkdir ["a"]]
               [:mkdir ["a" "b"]]
               [:truncate [["b"] 0]]
               [:mv [["b"] ["a" "b"]]]
               [:mkdir ["b"]]
               [:mv [["b"] ["a"]]]]))))

; Move a directory back and forth
(deftest mv-mv
  (expect [[:mkdir ["b"]]
           [:touch ["b" "b"]]
           [:mkdir ["a"]]
           [:mv [["b"] ["a"]]]
           [:mv [["a"] ["b"]]]]))

; We should be able to rm a file that's been lost thanks to an fsync loss--the
; missing inode shouldn't break later accesses
(deftest touch-lose-rm
  (expect [[:truncate [[:a] 0]]
           [:lose-unfsynced-writes nil]
           [:rm [:a]]]))

; A checker bug where losing un-fsynced files left dangling references to
; nonexistent inodes
(deftest write-lose-write-sync-lose
  (expect [[:truncate [["b"] 0]]
           [:lose-unfsynced-writes nil]
           [:append [["b"] "00"]]
           [:fsync ["b"]]
           [:lose-unfsynced-writes nil]
           [:read [["b"] "00"]]]))

; Truncating a synced file to extend it shouldn't zero its contents.
(deftest write-sync-lose-truncate
  (expect [[:append [["a"] "12"]]
           [:fsync ["a"]]
           [:read [["a"] "12"]]
           [:lose-unfsynced-writes nil]
           [:read [["a"] "12"]]
           [:truncate [["a"] 2]]
           [:read [["a"] "120000"]]]))
