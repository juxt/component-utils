(ns juxt.component-utils.scheduled-component
  (:require [clojure.core.async :refer [alts!! chan close! sliding-buffer timeout]]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]))

(defmacro named-thread
  "Create a named thread."
  [k & body]
  `(doto (Thread. (fn [] (try (log/debug "Starting thread" ~k)
                              ~@body
                              (catch Throwable t# (log/fatal t# "Scheduled thread died, this should NEVER happen.")))))
     (.setName (name ~k))
     (.setDaemon true)
     (.start)))

(defrecord ScheduledOperation [sync-interval scheduled-fn thread-name]
  component/Lifecycle
  (start [this]
    (let [control-chan (chan (sliding-buffer 1))]
      (named-thread
       (or thread-name :scheduled-thread)
       (loop []
         (let [[v c] (alts!! (remove nil? [control-chan (when sync-interval (timeout sync-interval))]))]
           ;; Exit when the control channel is closed
           (when-not (and (= control-chan c) (not v))
             (scheduled-fn this)
             (recur)))))
      (assoc this :control-chan control-chan)))
  (stop [{:keys [control-chan] :as this}]
    (when control-chan
      (close! control-chan))
    this))
