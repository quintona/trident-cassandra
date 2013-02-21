(defproject trident-cassandra "0.0.1-bucketwip1"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :javac-options     ["-target" "1.6" "-source" "1.6"]
  :dependencies [
                 [org.hectorclient/hector-core "1.1-1"
                  :exclusions [com.google.guava/guava]]
                ]

  :profiles {:dev {:dependencies [[storm "0.8.2"]
                     [org.clojure/clojure "1.4.0"]]}}
  :aot :all
  :min-lein-version "2.0.0"
)
