(ns jepsen.local-fs.util-test
  (:require [clojure.test.check [clojure-test :refer :all]
                                [generators :as g]
                                [properties :as prop]
                                [results :refer [Result]]]
            [jepsen.local-fs [util :refer :all]])
  (:import (java.util Arrays)))

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
