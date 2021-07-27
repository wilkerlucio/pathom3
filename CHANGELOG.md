# Changelog

## [2021.07.27-alpha]
- Support disable input destructuring validation on `pco/resolver` with the flag `::pco/disable-validate-input-destructuring?`
- Run `::pco/transform` before running the resolver validations
- Fixed bug when combining batch + disabled cache + missing outputs doing infinite loops

## [2021.07.23-alpha]
- Add `p.eql/process-one` and `p.a.eql/process-one` helpers

## [2021.07.19-alpha]
- BREAKING CHANGE: Strict mode by default, now errors surface quickly
- BREAKING CHANGE: Remove `remove-stats-plugin`, now must use the env flag `:com.wsscode.pathom3.connect.runner/omit-run-stats?` instead
- Optional lenient mode via setting `:com.wsscode.pathom3.error/lenient-mode? true` on env
- Boundary interface now accepts `:pathom/lenient-mode?` so the client can configure it
- `attribute-errors-plugin` is deprecated, when using lenient mode that behavior comes automatically
- Fix async runner reversing lists
- Add `ctry` helper to handle exceptions in sync and async at same time
- Foreign connection errors get wrapped to enrich the error context
- Add spec for `::pci/index-source-id`
- Detect cycles in nested inputs to prevent stack overflow at planner
- Support foreign unions
- Entities can decide union path via `::pf.eql/union-entry-key`

## [2021.07.10-1-alpha]
- Add more info to pom.xml to add repository links from clojars and cljdoc

## [2021.07.10-alpha]
- Add transit dependencies to fix cljdoc compilation

## [2021.07.9-alpha]
- Initial JAR release
