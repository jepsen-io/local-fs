(ns jepsen.local-fs.util-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as g]
                                [properties :as prop]
                                [results :refer [Result]]
                                [rose-tree :as rose]]
            [jepsen.local-fs [util :refer :all]])
  (:import (java.util Arrays)))

(deftest ^:focus gen-test
  (let [gen (history->gen [1 2 3 4])]
    (is (= '[[1 2 3 4]
             ([1 2] ([2] ([])) ([1] ([])))
             ([3 4] ([4] ([])) ([3] ([])))
             ([2 3 4]
              ([3 4] ([4] ([])) ([3] ([])))
              ([2 4] ([4] ([])) ([2] ([])))
              ([2 3] ([3] ([])) ([2] ([]))))
             ([1 3 4]
              ([3 4] ([4] ([])) ([3] ([])))
              ([1 4] ([4] ([])) ([1] ([])))
              ([1 3] ([3] ([])) ([1] ([]))))
             ([1 2 4]
              ([2 4] ([4] ([])) ([2] ([])))
              ([1 4] ([4] ([])) ([1] ([])))
              ([1 2] ([2] ([])) ([1] ([]))))
             ([1 2 3]
              ([2 3] ([3] ([])) ([2] ([])))
              ([1 3] ([3] ([])) ([1] ([])))
              ([1 2] ([2] ([])) ([1] ([]))))]
           (rose->data (gen->tree gen))))
    ;(pprint (rose/root (gen->tree gen)))
    ;(pprint (rose->data (gen->tree gen)))
    ))

(defspec hex->bytes-spec 1000
  (prop/for-all [bs g/bytes]
                (let [hex (bytes->hex bs)
                      bs' (hex->bytes hex)]
                  (reify Result
                    (pass? [_]
                      (Arrays/equals bs bs'))
                    (result-data [_]
                      {:hex  hex
                       :hex' (bytes->hex bs')})))))
