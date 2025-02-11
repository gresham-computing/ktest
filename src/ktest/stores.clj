(ns ktest.stores
  (:import (java.time
            Duration)
           (org.apache.kafka.streams
            TopologyInternalsAccessor)
           (org.apache.kafka.streams.processor
            StateStore)
           (org.apache.kafka.streams.processor.internals
            InternalTopologyBuilder
            InternalTopologyBuilder$StateStoreFactory)
           (org.apache.kafka.streams.state
            StoreBuilder
            Stores)
           (org.apache.kafka.streams.state.internals
            AbstractStoreBuilder
            KeyValueStoreBuilder
            SessionStoreBuilder
            StoreAccessor
            TimestampedKeyValueStoreBuilder
            TimestampedWindowStoreBuilder
            ValueAndTimestampDeserializer
            ValueAndTimestampSerde
            WindowStoreBuilder)))

(def ^:private store-factory-users-field
  (let [f (.getDeclaredField InternalTopologyBuilder$StateStoreFactory "users")]
    (.setAccessible f true)
    f))

(def ^:private store-factory-builder-field
  (let [f (.getDeclaredField InternalTopologyBuilder$StateStoreFactory "builder")]
    (.setAccessible f true)
    f))

(def ^:private store-factory-global-state-builders-field
  (let [f (.getDeclaredField InternalTopologyBuilder "globalStateBuilders")]
    (.setAccessible f true)
    f))

(defrecord SingletonStoreBuilder
  [^StoreBuilder sb ^StateStore s]

  StoreBuilder

  (build [_] s)


  (withCachingEnabled [this] this)


  (withCachingDisabled [this] this)


  (withLoggingEnabled [this _] this)


  (withLoggingDisabled [this] this)


  (logConfig [_] (.logConfig sb))


  (loggingEnabled [_] (.loggingEnabled sb))


  (name [_] (.name sb)))

(defmulti find-known-alternative
  (fn [builder _store-name] (type builder)))

(defmethod find-known-alternative :default
  [builder store-name]
  (println "Store [" store-name "] was of an unhandled type [" (type builder) "] and could not be sped up")
  builder)

(defmethod find-known-alternative SingletonStoreBuilder
  [builder _]
  builder)

(defn key-serde
  [^AbstractStoreBuilder builder]
  (StoreAccessor/keySerde builder))

(defn value-serde
  [^AbstractStoreBuilder builder]
  (StoreAccessor/valueSerde builder))

(defn de-timestamp-serde
  [^ValueAndTimestampSerde serde]
  (StoreAccessor/deTimestampSerde serde))

(defmethod find-known-alternative KeyValueStoreBuilder
  [builder store-name]
  (Stores/keyValueStoreBuilder
   (Stores/inMemoryKeyValueStore store-name)
   (key-serde builder) (value-serde builder)))

(defmethod find-known-alternative TimestampedKeyValueStoreBuilder
  [builder store-name]
  (Stores/timestampedKeyValueStoreBuilder
   (Stores/inMemoryKeyValueStore store-name)
   (key-serde builder) (de-timestamp-serde (value-serde builder))))

(defmethod find-known-alternative SessionStoreBuilder
  [builder store-name]
  (Stores/sessionStoreBuilder
   (Stores/inMemorySessionStore store-name
                                (Duration/ofMillis (.retentionPeriod builder)))
   (key-serde builder) (value-serde builder)))

;; can't be bothered to get window sizes currently

#_(defmethod find-known-alternative WindowStoreBuilder
    [builder store-name]
    (Stores/windowStoreBuilder
      (Stores/inMemoryKeyValueStore store-name)
      (key-serde builder) (value-serde builder)))

#_(defmethod find-known-alternative TimestampedWindowStoreBuilder
    [builder store-name]
    (Stores/timestampedWindowStoreBuilder
      (Stores/inMemoryWindowStore store-name)
      (key-serde builder) (value-serde builder)))

(defn alternative-store-builder
  [builder store-name]
  (.withLoggingDisabled (find-known-alternative builder store-name)))

(defn alternative-store
  [store-name ^InternalTopologyBuilder$StateStoreFactory state-store-factory]
  (let [builder (.get store-factory-builder-field state-store-factory)
        users (.get store-factory-users-field state-store-factory)
        alt (alternative-store-builder builder store-name)]
    {:name store-name
     :users (set users)
     :builder alt}))

(defn- singleton-store-builder
  [^StoreBuilder sb]
  (->SingletonStoreBuilder sb (.build sb)))

(defn share-global-stores
  [topology]
  (let [i-builder (TopologyInternalsAccessor/internalTopologyBuilder topology)
        global-store-builders (.get store-factory-global-state-builders-field i-builder)]
    (doseq [[n sb] global-store-builders]
      (.put global-store-builders n (singleton-store-builder (alternative-store-builder sb n))))
    topology))

(defn mutate-to-fast-stores
  [topology]
  (let [i-builder (TopologyInternalsAccessor/internalTopologyBuilder topology)
        store-factories (.stateStores ^InternalTopologyBuilder i-builder)]
    (doseq [store-name (keys store-factories)
            :let [{:keys [users builder]} (alternative-store store-name (get store-factories store-name))]]
      (.addStateStore i-builder builder true (into-array String users)))
    topology))
