# Changelog

## [Next]
- BREAKING CHANGE: Strict mode by default, now errors surface quickly
- BREAKING CHANGE: Remove `remove-stats-plugin`, now must use the env flag `:com.wsscode.pathom3.connect.runner/omit-run-stats?` instead
- Optional lenient mode via setting `:pathom/lenient-mode? true`
- Fix async runner reversing lists

## [2021.07.10-1]
- Add more info to pom.xml to add repository links from clojars and cljdoc

## [2021.07.10]
- Add transit dependencies to fix cljdoc compilation

## [2021.07.9]
- Initial JAR release
