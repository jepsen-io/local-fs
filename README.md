# jepsen.local-fs

Jepsen tests for local filesystems. Unlike most Jepsen tests, this runs purely
on your local node; no cluster required.

This works by [generating random histories of filesystem
operations](src/jepsen/local_fs/shell/workload.clj), [applying them to a real
filesystem](src/jepsen/local_fs/shell/client.clj), and then checking to see
whether the filesystem behaved like [a simulated, purely-functional
model](src/jepsen/local_fs/shell/checker.clj). When it finds a bug, it uses
Clojure's [test.check](https://github.com/clojure/test.check) to automatically
shrink the history to a minimal failing example.

That example includes a trace of the operations performed, shows you what state
it thought the filesystem was in, what it expected to perform, and what the
filesystem actually did. For instance, here's a bug we found in lazyfs where writes to hardlinks weren't reflected in the other links to that inode:

```clj
[:ok :append [["a"] ""]]
[:ok :ln [["a"] ["b"]]]
[:ok :append [["b"] "00"]]
[:ok :read [["a"] ""]]

At this point, the fs was theoretically
{:next-inode-number 1,
 :inodes {0 {:link-count 2, :data "00"}},
 :dir
 {:type :dir,
  :files {"a" {:type :link, :inode 0}, "b" {:type :link, :inode 0}}}}

And we expected to execute
{:f :read,
 :value [["a"] "00"],
 :time 18127561,
 :process 0,
 :type :ok,
 :index 7}

But with our filesystem we actually executed
{:f :read,
 :value [["a"] ""],
 :time 18127561,
 :process 0,
 :type :ok,
 :index 7}
```

Like all Jepsen tests, you'll find results, logs, and performance charts for
each test run in `store/`.

This is, unfortunately, a single-threaded test. I have *no* idea how to go
about modeling & checking POSIX filesystem safety under concurrent operations.
That said, single-threaded testing has been remarkably productive during lazyfs
development, so this might be useful for you too!

## Quickcheck

To check the local filesystem (using a directory called `data` in this repository):

```
lein run quickcheck
```

This automatically generates histories of operations, applies them to the
filesystem, and checks that they look OK. If it finds a bug, it'll try to
shrink that history to a minimal failing case.

To find a bug in lazyfs, run:

```
lein run quickcheck --db lazyfs --version be22191019619f3db7908b50fba500a3c9821884
```

To do this you'll need libfuse3-dev, fuse set up appropriately for lazyfs, gcc, etc, as well as leiningen. This will run a whole bunch of tests and spit out results in `store/`. You can browse these at the filesystem directly, or run

When you have a failing case, you might want to dive into it deeper. You can
replay a test like so:

```
lein run quickcheck --db lazyfs ... --history store/some-test/some-time/history.edn
```

This will retry the same operations from that EDN file. It'll go on to
aggressively shrink this history to a subset of operations, though it won't
shrink operations themselves. This might be a more efficient search than just
waiting for the initial `lein run quickcheck` to do its thing.

## Concurrent Tests

These will *definitely* not work correctly as far as safety testing is
concerned, but they might be neat from a crash-safety or performance
perspective. Try

```
lein run test --concurrency 10 --time-limit 30
```

This will run 10 IO threads which concurrently perform random operations. The
checker assumes histories are singlethreaded and will probably complain in
confusing ways. Pay it no mind.

## Browsing Results

Run

```
lein run serve
```

... which launches a web server on http://localhost:8080, serving up the contents of `store/`.


## License

Copyright © 2022 Jepsen, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
