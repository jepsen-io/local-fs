(ns jepsen.local-fs.db.core
  "Core protocols for databases.")

(defprotocol LoseUnfsyncedWrites
  (lose-unfsynced-writes! [db] "Causes this filesystem to lose un-fsynced writes."))
