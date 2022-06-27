# jepsen.local-fs

Jepsen tests for local filesystems. Unlike most Jepsen tests, this runs purely
on your local node; no cluster required.

## Usage

To check the local filesystem (using a directory called `data` in this repository):

```
lein run quickcheck
```

To find a bug in lazyfs, run

```
lein run quickcheck --db lazyfs --version be22191019619f3db7908b50fba500a3c9821884
```

To do this you'll need libfuse3-dev, fuse set up appropriately for lazyfs, gcc, etc, as well as leiningen. This will run a whole bunch of tests and spit out results in `store/`. You can browse these at the filesystem directly, or run

```
lein run serve
```

... which launches a web server on http://localhost:8080.

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
