# Changelog

## [NEXT]
- BREAKING: `::p.error/missing-output` is now converged to `::p.error/attribute-missing` (issue #149)
- BREAKING: Placeholders won't override entity data via params by default (issue #187)
- New `pbip/placeholder-data-params` plugin to get back the placeholder entity data behavior

## [2023.01.31-alpha]
- Fix map container handling on runner (issue #176)
- `pf.eql/data->shape` now takes `::pcr/map-container?` into account
- Disregard ident values on cache keys for the planner (issue #182)
- Fix stack overflow on planning nested attribute cycles on dynamic resolvers (issue #179)
- Fix name reporting on invalid config for resolvers and mutations (issue #181)
- Fix missing data on nested batches (issues #173 & #177)
- Fix planning issue when optimizing `OR` subpaths (issue #170)
- Index nested attributes (issue #167)

## [2023.01.24-alpha]
- Fix multiple inclusions of attribute error resolver (issue #175)
- Fix stack overflow on nested attribute cycles (issue #168)
- Fix batch waiting execution order (issue #169)

## [2022.10.19-alpha]
- Fix `:or` destructuring inference when mapping it to a symbol destructuring

## [2022.10.18-alpha]
- Fix error when user requests `::pcr/attribute-errors` in lenient mode
- In `process-one` helpers, when the value is collection it gets the run stats from the parent
- Add support for `:or` keyword on `defresolver` and `defmutation` args, values in `:or` will be flagged as optional
- Set `::pco/inferred-params` when params are inferred
- Instead of overriding `::pco/input` or `::pco/params` when explicitly set, now Pathom will merge that with the inferred input/params
- Issue 159 - Fix placeholder nodes on dynamic resolvers

## [2022.08.29-alpha]
- Fix ident processing on serial runner

## [2022.08.26-alpha]
- Fix planning optimization on merging sibling nodes with OR parent
- New branch optimizations are now opt-in via `::pcp/experimental-branch-optimizations` flag at env
- Fix `::pcr/wrap-mutation-error`, now working in all runners

## [2022.08.25-alpha] - BROKEN, DON'T USE
- Add support for `::pcr/wrap-merge-attribute` hook in async and parallel runners
- Add support for `::pcr/wrap-merge-attribute` hook on idents (all runners)
- Fix issue #152 missing shape check when data value is nil
- Optimize AND siblings with same branches
- Optimize OR siblings with same branches 🎉
- Optimize AND branch that has same branch structure as parent

## [2022.07.08-alpha]
- Fix: Exceptions that occur inside `p.a.eql/process` in JVM won't include the entire stacktrace anymore. (issue #142)
- Add `::p.error/error-data` to datafied error object
- Optimize nested AND branches on planner

## [2022.06.01-alpha]
- Fix nested optional key throws "All paths from an OR node failed" (issue #139)

## [2022.05.19-alpha]
- Fix spec issues with mutations
- Fix async environment on async boundary interface

## [2022.05.17-alpha]
- Fix nested check on deep dynamic resolver requirements
- Fix bad optimization when invalid sub-query isn't the first branch option of a path
- Fix multiple calls to same mutation (issue #137)
- BREAKING: Mutations are not part of `index-ast` anymore
- BREAKING: `::pcp/mutations` now contains the full call AST instead of the mutation key
- Fix Optional key throws "All paths from an OR node failed" (issue #138)

## [2022.04.20-alpha]
- Fix unnecessary resolver calls due to merging OR branches optimization
- Add check on OR nodes to test for expectation done before start running
- Upgrade guardrails to have its own Kondo configuration

## [2022.03.17-alpha]
- Fix boundary interface detection of lenient mode to output stats

## [2022.03.07-alpha]
- Run stats now includes details for transient (implicit) dependencies as well
- Improve parallelism with placeholders
- Placeholders with params are deal as a new entity (instead of an extension of the parent)

## [2022.03.04-alpha]
- Add support for `::pco/batch-chunk-size` to how many items to batch at once
- Batch process retains input order when making batch calls

## [2022.02.25-alpha]
- `::pcp/expects` now can figure implicit attributes on static resolver nested lookup
- Planner now removes path options that can't fulfill the sub-query
- New priority sort algorithm. Thanks to @mszabo!
- Optimize nested or nodes, pulling nested branches into parent OR node
- Add weight sort feature to load balance between OR branches
- Fix issue #107 regarding batch + nested optional inputs

## [2022.02.24-alpha]
- Add `pf.eql/seq-data->query` to find shape of a combined collection
- Computation of `::pcp/expects` on static resolvers now includes data about nested expectations

## [2022.02.21-alpha]
- Bumped Promesa to 6.1.436
- INTERNAL BREAK: the internal `plan-and-run!` from all runners now return env instead of the graph plan
- Runner exceptions now return a wrapped error that includes environment data
- Boundary interface errors are always data
- Boundary interface omit stats by default, open with the :pathom/include-stats? flag on the request
- Add `pbip/env-wrap-plugin`

## [2022.02.01-1-alpha]
- Remove debugging tap

## [2022.02.01-alpha]
- Fix error trigger when a dependency of some optional attribute fails (bug #126)
- Fix planning errors on optional nested inputs (bug #127)

## [2022.01.28-alpha]
- Improve optimization for single item branch, now support moving run-next to the child's edge
- On union process, in case the item doesn't match any branch its turned into nil (and removed in case of union sequences)
- Fix bug when checking for entity required data, OR cases could trigger a false positive

## [2022.01.09-alpha]
- Fix `Throwable` warning on CLJS in parallel parser
- Fix `log` source to avoid `*file*` warning

## [2021.12.10-alpha]
- Add `pbip/dev-linter` to help find errors in operation definitions
- Improve performance on input shape check
- Fix error handling for batch resolvers on parallel processor

## [2021.11.16-alpha]
- Add extension point `::p.error/wrap-attribute-error`
- Fix batch calls with distinct parameters
- Add `pco/final-value` helper to mark a value as final

## [2021.10.20-alpha]
- Add optimizations to OR branches that are sub-paths of each other
- Support new `::pco/cache-key` setting for custom cache keys at resolver level

## [2021.10.19-alpha]
- Fix foreign mutation on async runner
- Remove viz request snapshots to allow multiple connectors on the same meshed graph
- Fix the flow of params on foreign-ast
- Fix nested dependency process
- Infer output shape in constantly resolver
- Parallel processor 🎉

## [2021.08.14-alpha]
- Fix cache store specs
- Add `::pcr/wrap-process-sequence-item` plugin entry point
- Add `filtered-sequence-items-plugin` new built-in plugin
- Batch support for dynamic resolvers
- Batch support on foreign requests
- BREAKING CHANGE: Dynamic resolvers always get rich inputs with input data and foreign ast
- Dynamic mutations also go as input parts with params 

## [2021.07.30-alpha]
- Fix lenient mode optional inputs, it was marking errors when it shouldn't

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
