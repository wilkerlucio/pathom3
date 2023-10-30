(ns tasks
  (:require
    [babashka.deps :as deps]
    [babashka.fs :as fs]
    [babashka.process :as p]
    [clojure.data.xml :as xml]
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
    (apply clojure "-Sdeps" "{:aliases {:cljstyle {:replace-deps {mvxcvi/cljstyle {:mvn/version \"0.15.0\"}}}}}" "-M:cljstyle" "-m" "cljstyle.main" args)))

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

; region git

(defn git-tags []
  (into #{} (str/split-lines (sh-out "git" "tag" "-l"))))

(defn create-tag! [version]
  (sh "git" "tag" "-a" version "-m" version))

(defn push-with-tags []
  (sh "git" "push" "--follow-tags"))

; endregion

; region pom.xml

(defn update-child-tag [element tag f]
  (update element :content
    #(mapv
       (fn [element]
         (if (= tag (:tag element))
           (f element)
           element))
       %)))

(defn update-pom-scm-tag [new-tag]
  (-> (xml/parse-str (slurp "pom.xml"))
      (update-child-tag :xmlns.http%3A%2F%2Fmaven.apache.org%2FPOM%2F4.0.0/scm
                        (fn [scm-el]
                          (update-child-tag scm-el :xmlns.http%3A%2F%2Fmaven.apache.org%2FPOM%2F4.0.0/tag
                                            #(assoc % :content [new-tag]))))
      (xml/emit-str)
      (->> (spit "pom.xml"))))

; endregion

; region artifact

(defn current-version []
  (if (fs/exists? "VERSION")
    (str/trim (slurp "VERSION"))))

(defn artifact-path []
  (str "target/pathom3-" (tasks/current-version) ".jar"))

(defn version-tag
  ([] (version-tag (current-version)))
  ([version] (str "v" version)))

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
     (str (if (= date today)
            (str date "-" (or (some-> iteration Integer/parseInt inc) 1))
            today)
          "-alpha"))))

(defn released?
  ([] (released? (current-version)))
  ([version]
   (contains? (git-tags) (version-tag version))))

(defn str-arg [s]
  (pr-str (str s)))

(defn artifact-build
  ([]
   (clojure "-X:jar" ":jar" (artifact-path) ":version" (str-arg (current-version)))))

(defn artifact-deploy
  ([] (artifact-deploy (artifact-path)))
  ([artifact]
   (clojure "-X:deploy" ":artifact" (str-arg artifact))))

(defn bump! []
  (let [version   (next-version)
        changelog (slurp "CHANGELOG.md")]
    (if (re-find #"\[NEXT]" changelog)
      (let [changelog' (str/replace changelog #"\[NEXT]" (str "[" version "]"))]
        (spit "VERSION" version)
        (spit "CHANGELOG.md" changelog'))
      (throw (ex-info "CHANGELOG.md must have a [NEXT] mark." {})))
    version))

; endregion
