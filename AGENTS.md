# Project Agents.md Guide

This is a [MoonBit](https://docs.moonbitlang.com) project.

You can browse and install extra skills here:
<https://github.com/moonbitlang/skills>

## Project Structure

- MoonBit packages are organized per directory; each directory contains a
  `moon.pkg` file listing its dependencies. Each package has its files and
  blackbox test files (ending in `_test.mbt`) and whitebox test files (ending in
  `_wbtest.mbt`).

- In the toplevel directory, there is a `moon.mod` file listing module
  metadata.

## Coding convention

- MoonBit code is organized in block style, each block is separated by `///|`,
  the order of each block is irrelevant. In some refactorings, you can process
  block by block independently.

- Try to keep deprecated blocks in file called `deprecated.mbt` in each
  directory.

## Commit messages

- Keep the subject short. If a body is needed, write a simple paragraph
  describing the change; do not list implementation details.

## Versioning

- Every change that touches a version number gets a commit of its own. Bumping
  `VERSION`, `moon.mod`, or the `CHANGELOG.md` release header is never mixed
  into a commit made for something else.

- This holds even when the other commit is still local and unpushed: amend is
  not an exception. Cut a new commit for the version bump.

## Pushing

- `git push` is a serious, outward-facing operation. Only ever run it after an
  explicit instruction from the user, and never push more often than the
  threshold below allows.

- Right after committing, count how many commits the remote does not yet have:

  ```
  git rev-list --count origin/main..HEAD
  ```

  (A branch with an upstream configured can use `@{upstream}..HEAD` instead, but
  this repo's feature branches generally have none — `git rev-parse @{upstream}`
  just fails there — so the explicit `origin/main` is the reliable form.)

  - **10 or fewer** — stop. Do not push, and do not ask whether to push.
  - **More than 10** — ask the user whether to push, and wait for their
    confirmation before running it.

- The count is of unpushed commits, not of the branch's total history. A commit
  made on a branch that is already in sync is 1, which is never a reason to ask.

## Tooling

- `moon build` does not work at the module level — use `moon check` to
  type-check the whole library (the Makefile's `build` target is exactly that).
  The root package and `compression` are libraries that declare
  `options(link: "-lz")` for their test binaries, and `moon build` tries to link
  both as executables (`moonkafka.exe`, `compression.exe`), dying on the missing
  `_main`. Do not "fix" this by dropping the option: `moon test` then fails to
  link against zlib. The actual executables (`cmd/main`, `docs/demo`) carry
  their own `-lz` and do build on their own.

- The module is native-only, so `moon check --target all` fails too, reporting
  unbound identifiers in `compression`, whose FFI is gated to native.

- `moon fmt` is used to format your code properly.

- `moon ide` provides project navigation helpers like `peek-def`, `outline`, and
  `find-references`. See $moonbit-agent-guide for details.

- `moon info` is used to update the generated interface of the package, each
  package has a generated interface file `.mbti`, it is a brief formal
  description of the package. If nothing in `.mbti` changes, this means your
  change does not bring the visible changes to the external package users, it is
  typically a safe refactoring.

- In the last step, run `moon info && moon fmt` to update the interface and
  format the code. Check the diffs of `.mbti` file to see if the changes are
  expected.

- Run `moon test` to check tests pass. MoonBit supports snapshot testing; when
  changes affect outputs, run `moon test --update` to refresh snapshots.

- Prefer `assert_eq` or `assert_true(pattern is Pattern(...))` for results that
  are stable or very unlikely to change. For snapshot tests that record
  structured debugging output, derive `Debug` and use `debug_inspect`, rather
  than deriving `Show` for debugging. For solid, well-defined results (e.g.
  scientific computations), prefer assertion tests. You can use
  `moon coverage analyze > uncovered.log` to see which parts of your code are
  not covered by tests.

- `sed -i` is a portability trap on macOS. `/usr/bin/sed` is BSD sed, whose
  `-i` takes an explicit extension argument, making the BSD idiom
  `sed -i '' 's/a/b/' f`. GNU sed instead reads that empty argument as the
  *script* and dies with `can't read s/a/b/`. Homebrew's `gnu-sed` provides
  `gsed`, which is always GNU sed; it also installs a `gnubin/sed`, and where
  that directory precedes `/usr/bin` on `PATH` the plain `sed` is GNU too — on
  this machine it is, so the BSD idiom above is the one that fails here. Check
  `sed --version` ("GNU sed" versus an illegal-option usage error) when the
  dialect matters, or write `gsed` to be sure of GNU semantics.

- Do not `sed -i` a file you are about to commit. Use the editor's exact-match
  edit instead: a mis-parsed script fails loudly, but a subtly wrong pattern
  rewrites the file and the commit goes through unnoticed.
