(ns ktest.internal.interop
  (:import (java.lang.invoke MethodHandles VarHandle)
           (java.lang.reflect
             Field
             Modifier)
           (java.util
             Properties)
           (org.apache.kafka.streams
             Topology
             TopologyInternalsAccessor
             TopologyTestDriver)
           (org.apache.kafka.streams.processor.internals
             CapturingStreamTask
             StreamTask)))

(defn- properties
  [p]
  (reduce-kv
    (fn [p k v]
      (doto p
        (.setProperty (name k) v)))
    (Properties.)
    p))

(defn test-driver
  [topology config epoch-millis output-capture]
  (let [t (TopologyTestDriver. ^Topology topology
                               ^Properties (properties config)
                               ^Long epoch-millis)
        tia (TopologyInternalsAccessor.)
        task (.getTestStreamTask tia t)
        wrapped-task (when task (CapturingStreamTask. task output-capture))]
    (.setTestStreamTask tia t wrapped-task)
    t))
