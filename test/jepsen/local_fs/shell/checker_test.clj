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

