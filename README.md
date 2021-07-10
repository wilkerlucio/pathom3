# Pathom 3 [![Clojars Project](https://img.shields.io/clojars/v/com.wsscode/pathom3.svg)](https://clojars.org/com.wsscode/pathom3) ![Test](https://github.com/wilkerlucio/pathom3/workflows/Test/badge.svg) [![cljdoc badge](https://cljdoc.xyz/badge/com.wsscode/pathom3)](https://cljdoc.xyz/d/com.wsscode/pathom3/CURRENT)

![Pathom Logo](repo-resources/pathom-banner-padded.png)

Logic engine for attribute processing for Clojure and Clojurescript.

Pathom3 is a redesign of Pathom, this is a new library and uses different namespaces.

## Status

Alpha, changes and breakages may occur. Recommended for enthusiasts and people looking to help
and chasing bugs and help improve the development of Pathom.

## Install

While in alpha state, this library is only available as a git deps dependency.

```clojure
com.wsscode/pathom3 {:mvn/version "2021.07.10-alpha"}
```

## Documentation

https://pathom3.wsscode.com/

## Run Tests

Pathom 3 uses [Babashka](https://github.com/babashka/babashka) for task scripts, please install it before proceed.

### Clojure

```shell script
bb test
```

### ClojureScript

To run once

```shell script
bb test-cljs-once
```

Or to start shadow watch and test in the browser:

```shell script
bb test-cljs
```
