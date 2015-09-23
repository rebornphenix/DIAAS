(defproject autoscaler "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.apache.curator/curator-recipes "2.8.0"]
                 [org.apache.curator/curator-test "2.8.0"]
                 [org.apache.curator/curator-x-discovery "2.8.0"]
                 [org.slf4j/slf4j-api "1.7.5"]
                 [org.slf4j/slf4j-log4j12 "1.7.5"]
                 [com.google.guava/guava "15.0"]
                 [commons-cli/commons-cli "1.2"]
                 [org.apache.commons/commons-exec "1.3"]
                 [org.apache.commons/commons-exec "1.3"]
                 [commons-lang/commons-lang "2.6"]
                 [com.github.sgroschupf/zkclient "0.1"]
                 [org.apache.helix/helix-core "0.6.5"]
                 [org.apache.zookeeper/zookeeper "3.4.6" :exclusions [junit/junit]]
                 [org.codehaus.jackson/jackson-core-asl "1.8.5"]
                 [org.codehaus.jackson/jackson-mapper-asl "1.8.5"]
                 [com.github.jsimone/webapp-runner "7.0.57.1"]
                 [org.codehaus.jettison/jettison "1.1"]
                 [com.google.code.gson/gson "2.2.2"]
                 [org.eclipse.jetty/jetty-server "9.0.2.v20130417"]
                 [org.apache.commons/commons-collections4 "4.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 ]
  :source-paths ["src/main/clojure"]
  :java-source-paths ["src/main/java"]
  :test-paths ["src/test/java" "src/test/clojure"]
  :resource-paths ["src/main/resource"]
  :main ^:skip-aot autoscaler.core
  :jvm-opts ["-Xmx1g"]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :test    {:dependencies [[junit/junit "4.11"]
                                      [org.hamcrest/hamcrest-core "1.3"]
                                      [org.hamcrest/hamcrest-library "1.3"]
                                      [org.mockito/mockito-core "1.9.5"]]

                       :test-resource-paths ["src/test/resource"]}})
