# Contributing to Taskite

## Before contributing

Discuss substantial changes in an issue before implementation. Keep changes
focused, include appropriate tests, and run:

```bash
mvn clean verify
```

The Maven license plugin adds the notice from
[`licenses/license-header`](licenses/license-header) to new source files during
the `initialize` phase and verifies all managed headers during `verify`.

## Contribution license

All contributions are governed by
[`CLA.md`](CLA.md), Version 1.0. Every commit in a pull request must contain:

```text
Signed-off-by: Contributor Name <contributor@example.com>
Taskite-CLA: 1.0
```

The repository CLA check rejects a pull request when either trailer is
missing. Repository administrators must configure `Verify CLA trailers` as a
required branch-protection check and must not bypass it. Do not add a trailer
on another person's behalf.

If your employer or another party may own rights in your contribution, obtain
written authorization before submitting it. Identify all third-party material
and its license in the pull request. Third-party material is not covered by
the CLA and must be compatible with Taskite's community and commercial
distribution models.

Community participation is also governed by
[`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md).
