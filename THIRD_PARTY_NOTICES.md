# Third-Party Notices

The Taskite Community License applies only to material for which a Taskite
copyright holder has authority to grant rights. Third-party materials retain
their original copyright and license terms.

## Runtime and build dependencies

Taskite uses third-party Maven dependencies, including Apache Pekko, Agrona,
cron-utils, Scala, gRPC, Protocol Buffers, Jackson, JUnit, Logback, Micrometer, Prometheus,
RocksDB, SOFAJRaft, Spring Boot, and their transitive dependencies.

The Go client and `tkctl` use Cobra, pflag, gRPC-Go, protobuf-go,
`google.golang.org/genproto`, and the `golang.org/x/net`, `x/sys`, and `x/text`
modules. Their transitive dependency `mousetrap` is used on Windows. These
components remain governed by their Apache License 2.0 or BSD-style licenses
as declared by their respective modules.

The Taskite Community License does not alter those terms. Every release build
generates a dependency-to-license inventory and extracts each dependency's
embedded legal files into an artifact-specific directory. Canonical texts are
included for dependencies that do not embed the required license. The
distribution JAR contains them under:

```text
META-INF/taskite/third-party/
|-- THIRD-PARTY.txt
|-- artifacts/
`-- canonical/
```

The merged third-party attribution notices remain available at
`META-INF/NOTICE`. `META-INF/LICENSE` contains the Taskite Community License
and must not be interpreted as replacing any third-party license.

The canonical supplemental texts cover:

- Protocol Buffers 3.25.5: BSD 3-Clause;
- ASM 6.0: BSD 3-Clause;
- Animal Sniffer Annotations 1.24: MIT; and
- Logback 1.5.x: Eclipse Public License 1.0, selected from Logback's
  EPL-1.0/LGPL-2.1 dual license.

Where a dependency offers Apache License 2.0 as one of multiple alternatives,
including RocksDB JNI and JNA, Taskite distributes that dependency under the
Apache License 2.0 alternative.

The build fails when a dependency license cannot be resolved. Before a public
release, inspect the generated inventory for incompatible or unexpected
licenses and retain all generated files when redistributing the binary.
