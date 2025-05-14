(ns ktest.stores
  (:import (java.time Duration)
           (java.util List)
           (org.apache.kafka.common.utils Time)
           (org.apache.kafka.streams
             TopologyInternalsAccessor)
           (org.apache.kafka.streams.kstream.internals KeyValueStoreMaterializer MaterializedStoreFactory MaterializedStoreFactoryAccessor)
           (org.apache.kafka.streams.processor
             ProcessorContext StateRestoreCallback StateStore)
           (org.apache.kafka.streams.processor.internals
             InternalTopologyBuilder StoreBuilderWrapper StoreFactory)
           (org.apache.kafka.streams.state
             Stores TimestampedKeyValueStore)
           (org.apache.kafka.streams.state.internals AbstractStoreBuilder CachedStateStore KeyValueStoreBuilder SessionStoreBuilder StoreAccessor TimestampedKeyValueStoreBuilder ValueAndTimestampSerde WrappedStateStore)))

(def ^:private global-state-builders-field
  (let [f (.getDeclaredField InternalTopologyBuilder "globalStateBuilders")]
    (.setAccessible f true)
    f))

(def ^:private store-factory-builder-field
  (let [f (.getDeclaredField StoreBuilderWrapper "builder")]
    (.setAccessible f true)
    f))

(defrecord SingletonStoreFactory
  [^StoreFactory sf build]

  StoreFactory

  (build [_] (build))

  (configure [_this config] (.configure sf config))

  (retentionPeriod [_this] (.retentionPeriod sf))

  (historyRetention [_this] (.historyRetention sf))

  (connectedProcessorNames [_this] (.connectedProcessorNames sf))

  (loggingEnabled [_this] (.loggingEnabled sf))

  (name [_this] (.name sf))

  (isWindowStore [_this] (.isWindowStore  sf))

  (isVersionedStore [_this] (.isVersionedStore sf))

  (logConfig [_this] (.logConfig sf))

  (withCachingDisabled [_this] (.withCachingDisabled sf))

  (withLoggingDisabled [_this] (.withLoggingDisabled sf))

  (isCompatibleWith [_this v] (.isCompatibleWith sf v)))

(defn- singleton-store-builder
  [^StoreFactory sb]
  (let [inner-store (atom nil)]
    (->SingletonStoreFactory
      sb
      (fn []
        (when-not (and @inner-store
                       (.isOpen @inner-store))
          (reset! inner-store (.build sb)))
        (cond
          (isa? (type @inner-store) TimestampedKeyValueStore)
          (proxy
            [WrappedStateStore TimestampedKeyValueStore CachedStateStore]
            [@inner-store]
            (^void init [^ProcessorContext v1 ^StateStore v2]
              (if (.isOpen @inner-store)
                (.register v1 this (reify StateRestoreCallback (restore [_this _a _b])))
                (.init @inner-store v1 v2)))
            (flush [] (.flush @inner-store))
            (close [] (.close @inner-store))
            (persistent [] (.persistent @inner-store))
            (isOpen [] (.isOpen @inner-store))
            (name [] (.name @inner-store))
            (get [k] (.get @inner-store k))
            (range [k1 k2] (.range @inner-store k1 k2))
            (reverseRange [k1 k2] (.reverseRange @inner-store k1 k2))
            (all [] (.all @inner-store))
            (prefixScan [v1 v2] (.prefixScan @inner-store v1 v2))
            (put [k v] (.put @inner-store k v))
            (putIfAbsent [k v] (.putIfAbsent @inner-store k v))
            (^void putAll [^List kvs] (.putAll @inner-store kvs))
            (delete [k] (.delete @inner-store k))
            (approximateNumEntries [] (.approximateNumEntries @inner-store))
            (setFlushListener [listener, send-old-values] (.setFlushListener ^WrappedStateStore @inner-store listener send-old-values))
            (flushCache [] (.flushCache @inner-store))
            (clearCache [] (.clearCache @inner-store))
            (wrapped [] (do @inner-store)))

          :else
          (throw (Exception. "Unsupported factory type")))))))

(defn builder->key-serde
  [^AbstractStoreBuilder builder]
  (StoreAccessor/keySerde builder))

(defn builder->value-serde
  [^AbstractStoreBuilder builder]
  (StoreAccessor/valueSerde builder))

(defn de-timestamp-serde
  [^ValueAndTimestampSerde serde]
  (StoreAccessor/deTimestampSerde serde))

(defmulti find-store-builder-alternative
          (fn [^AbstractStoreBuilder store-builder _store-name]
            (type store-builder)))

(defmethod find-store-builder-alternative :default
  [builder store-name]
  (println "Store Builder [" store-name "] was of an unhandled type [" (type builder) "] and could not be sped up")
  builder)

(defmethod find-store-builder-alternative KeyValueStoreBuilder
  [builder store-name]
  (Stores/keyValueStoreBuilder
    (Stores/inMemoryKeyValueStore store-name)
    (builder->key-serde builder) (builder->value-serde builder)))

(defmethod find-store-builder-alternative TimestampedKeyValueStoreBuilder
  [builder store-name]
  (Stores/timestampedKeyValueStoreBuilder
    (Stores/inMemoryKeyValueStore store-name)
    (builder->key-serde builder) (de-timestamp-serde (builder->value-serde builder))))

(defmulti find-store-factory-alternative
          (fn [^StoreFactory store-factory _store-name]
            (type store-factory)))

(defmethod find-store-factory-alternative :default
  [factory store-name]
  (println "Store Factory [" store-name "] was of an unhandled type [" (type factory) "] and could not be sped up")
  factory)

(defmethod find-store-factory-alternative SingletonStoreFactory
  [store-factory _store-name]
  store-factory)

(defmethod find-store-factory-alternative StoreBuilderWrapper
  [store-factory store-name]
  (-> (.get store-factory-builder-field store-factory)
      (find-store-builder-alternative store-name)
      (StoreBuilderWrapper.)))

(defn materialised->key-serde
  [^MaterializedStoreFactory builder]
  (MaterializedStoreFactoryAccessor/keySerde builder))

(defn materialised->value-serde
  [^MaterializedStoreFactory builder]
  (MaterializedStoreFactoryAccessor/valueSerde builder))

(defmethod find-store-factory-alternative KeyValueStoreMaterializer
  [^KeyValueStoreMaterializer store-factory store-name]
  (StoreBuilderWrapper.
    (TimestampedKeyValueStoreBuilder.
      (Stores/inMemoryKeyValueStore store-name)
      (materialised->key-serde store-factory)
      (materialised->value-serde store-factory)
      Time/SYSTEM)))

(defn alternative-store
  [store-name ^StoreFactory state-store-factory]
  (-> (find-store-factory-alternative state-store-factory store-name)
      (.withLoggingDisabled)))

(defn share-global-stores
  [topology]
  (let [i-builder (TopologyInternalsAccessor/internalTopologyBuilder topology)
        global-store-builders (.get global-state-builders-field i-builder)]
    (doseq [[n sb] global-store-builders]
      (when-not (isa? (type sb) SingletonStoreFactory)
        (.put global-store-builders n (singleton-store-builder sb)))))
  topology)

(defn mutate-to-fast-stores
  [topology]
  (let [i-builder (TopologyInternalsAccessor/internalTopologyBuilder topology)
        store-factories (.stateStores ^InternalTopologyBuilder i-builder)]
    (doseq [store-name (keys store-factories)]
      (let [^StoreFactory original-factory (get store-factories store-name)]
        (let [^StoreFactory replacement-factory (alternative-store store-name original-factory)]
          (.addStateStore
            i-builder
            replacement-factory
            true
            (->> (.connectedProcessorNames original-factory)
                 (into-array String))))))
    topology))