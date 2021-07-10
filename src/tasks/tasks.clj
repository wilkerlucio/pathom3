(ns tasks
  (:require
    [babashka.deps :as deps]
    [babashka.process :as p]
    [clojure.string :as str])
  (:import
    (java.time
      LocalDate)
    (java.time.format
      DateTimeFormatter)))

; region helpers

(defn check [p]
  (try
    (p/check p)
    (catch Throwable _
      (System/exit 1))))

(defn- sh-dispatch [args opts]
  (let [ps (if (= (first args) :clojure)
             (deps/clojure (map name (rest args)) opts)
             (p/process (map name args) opts))]
    ps))

(defn- sh [& args]
  (println "=>" (str/join " " (map name args)))
  (check (sh-dispatch args {:inherit true}))
  nil)

(defn- sh-silent [& args]
  (sh-dispatch args {}))

(defn- sh-out [& args]
  (-> (sh-dispatch args {:out :string})
      check
      :out))

(def clojure (partial sh :clojure))

; endregion

; region cljstyle

(defn native-cljstyle? []
  (-> (sh-silent :which "cljstyle") deref :exit zero?))

(def cljstyle-native (partial sh :cljstyle))

(defn cljstyle [& args]
  (if (native-cljstyle?)
    (apply cljstyle-native args)
    (apply clojure "-Sdeps" "{:deps {mvxcvi/cljstyle {:mvn/version \"0.15.0\"}}}" "-m" "cljstyle.main" args)))

; endregion

; region clj-kondo

(def clj-kondo (partial sh :clj-kondo))

(defn clj-kondo-lint [paths]
  (apply clj-kondo "--lint" paths))

; endregion

; region git

(defn modified-files
  "Return a list of the files that are part of the current commit.
  Each item is a string with the file path."
  []
  (-> (sh-out "git" "diff" "--cached" "--name-only" "--diff-filter=ACMR")
      str/split-lines))

(defn update-file-index
  "Add unstaged modifications to git, so they get to be part of the current commit."
  [path]
  (let [hash (sh-out "git" "hash-object" "-w" path)]
    (sh-silent :git "update-index" "--add" "--cacheinfo" "100644" hash path)))

; endregion

; region pre commit action to validate and format

(defn clojure-source? [path]
  (re-find #"\.(clj|cljs|cljc)$" path))

(defn setup-git-hooks
  "Create a small script at git pre-commit path to trigger the pre-commit function inside the tasks namespace."
  []
  (spit ".git/hooks/pre-commit" "#!/usr/bin/env bb -m tasks/pre-commit\n")
  (sh :chmod "+x" ".git/hooks/pre-commit")
  (println "Setup .git/hooks/pre-commit done."))

(defn pre-commit
  "Script to run at Git pre-commit phase."
  [& _args]
  (let [paths (->> (modified-files)
                   (filter clojure-source?))]
    (when (seq paths)
      ; fix format
      (apply cljstyle "fix" paths)

      (doseq [path paths]
        (update-file-index path))

      (clj-kondo-lint paths))))

; endregion

; region version

(defn current-version []
  (str/trim (slurp "VERSION")))

(defn- current-date []
  (str/replace
    (.format (LocalDate/now) DateTimeFormatter/ISO_LOCAL_DATE)
    "-"
    "."))

(defn next-version
  ([] (next-version (current-version)))
  ([current-version]
   (let [today (current-date)
         [_ date iteration] (re-find #"(\d{4}\.\d{2}\.\d{2})(?:-(\d+))?" (or current-version ""))]
     (if (= date today)
       (str date "-" (or (some-> iteration Integer/parseInt inc) 1))
       today))))

(defn bump! []
  (let [version (next-version)]
    (spit "VERSION" version)
    version))

; endregion
