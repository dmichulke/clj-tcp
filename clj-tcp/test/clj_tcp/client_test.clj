(ns clj-tcp.client-test
  (:require [clj-tcp.client :as c]
            [clojure.test :refer [is deftest testing run-tests]]
            [net.tcp.server :as s])
  (:import [java.io Reader Writer BufferedReader]))


(def seconds 20)

(defn- server-handler [^BufferedReader reader ^Writer writer]
  (println "starting server")
  (let [state (atom "undefined")]
    (try
      (dotimes [i seconds]
        (println "server" i)
        (when (.ready reader)
          (println "server received something")
          (let [recv (.readLine reader)]
            (println "server received" recv)
            (reset! state recv)))
        (.append writer ^String @state)
        (.flush writer)
        (Thread/sleep 1000))
      (catch Throwable t (do (.printStackTrace t)
                             (throw t)))
      (finally (println "server stopped")))))

(deftest test-long-running-session
  (let [port 5000
        srv (s/tcp-server
             :port port
             :handler (s/wrap-io server-handler))]
    (try
      (s/start srv)
      (let [c (c/client "localhost" port {})
            change-at 10]
        (dotimes [i seconds]
          (do (println "Client" i)
              (let [recv (when-let [x (c/read! c 100)]
                           (slurp x))
                    expected (if (<= i change-at) "undefined" "hello")]
                (is (= expected recv)))
              (when (== i change-at)
                (c/write! c "hello\n")))
          (Thread/sleep 1000)))
      (finally (s/stop srv)))))
