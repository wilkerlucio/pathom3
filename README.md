# Pathom 3

![Pathom Logo](repo-resources/pathom-banner-padded.png)

Logic engine for attribute processing for Clojure and Clojurescript.

Pathom3 is a redesign of Pathom, this is a new library and uses different namespaces.

## Status

Alpha, avoid for critical parts. Recommended for enthusiasts and people looking to help
and chasing bugs and help improve the development of Pathom.

## Install

While in alpha state, this library is only available as a git deps dependency.

```clojure
{:deps {com.wsscode/pathom3 {:git/url "https://github.com/wilkerlucio/pathom3"
                             :sha     "FIND_LATEST_ON_GITHUB"}}}
```

## Documentation

https://pathom3.wsscode.com/

## Run Tests

### Clojure

```shell script
./script/test
```

### ClojureScript

```shell script
npm install
npx shadow-cljs compile ci
npx karma start --single-run
```
