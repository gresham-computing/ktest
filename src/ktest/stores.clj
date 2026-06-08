(ns ktest.stores
  (:import (java.lang.reflect
            Field)
           (java.util
            List
            Map)
           (org.apache.kafka.common.serialization
            Serializer)
           (org.apache.kafka.common.utils
            Time)
           (org.apache.kafka.streams
            TopologyInternalsAccessor)
           (org.apache.kafka.streams.kstream.internals
            KeyValueStoreMaterializer
            MaterializedStoreFactory
            MaterializedStoreFactoryAccessor)
           (org.apache.kafka.streams.processor
            ProcessorContext
            StateRestoreCallback
            StateStore
            StateStoreContext)
           (org.apache.kafka.streams.processor.internals
            InternalTopologyBuilder
            StoreBuilderWrapper
            StoreFactory)
           (org.apache.kafka.streams.query
            Position
            PositionBound
            Query
            QueryConfig
            QueryResult)
           (org.apache.kafka.streams.state
            KeyValueIterator
            KeyValueStore
            ReadOnlyKeyValueStore
            Stores
            TimestampedKeyValueStore)
           (org.apache.kafka.streams.state.internals
            AbstractStoreBuilder
            CacheFlushListener
            CachedStateStore
            KeyValueStoreBuilder
            MeteredKeyValueStore
            StoreAccessor
            TimestampedKeyValueStoreBuilder
            ValueAndTimestampSerde
            WrappedStateStore)))

(def ^:private ^Field global-state-builders-field
  (let [^Field f (.getDeclaredField InternalTopologyBuilder "globalStateBuilders")]
    (.setAccessible f true)
    f))

(def ^:private ^Field store-factory-builder-field
  (let [^Field f (.getDeclaredField StoreBuilderWrapper "builder")]
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
                      (.isOpen ^StateStore @inner-store))
         (reset! inner-store (.build sb)))
       (cond
         (isa? (type @inner-store) TimestampedKeyValueStore)
         (proxy [WrappedStateStore TimestampedKeyValueStore CachedStateStore]
                [@inner-store]
           (^void init [^ProcessorContext v1 ^StateStore v2]
             (if (.isOpen ^StateStore @inner-store)
               (.register v1 this (reify StateRestoreCallback
                                    (restore [_this _a _b])))
               (.init ^TimestampedKeyValueStore @inner-store v1 v2)))

           (flush [] (.flush ^TimestampedKeyValueStore @inner-store))

           (close [] (.close ^TimestampedKeyValueStore @inner-store))

           (persistent [] (.persistent ^StateStore @inner-store))

           (isOpen [] (.isOpen ^StateStore @inner-store))

           (name [] (.name ^StateStore @inner-store))

           (get [k] (.get ^TimestampedKeyValueStore @inner-store k))

           (range [k1 k2] (.range ^TimestampedKeyValueStore @inner-store k1 k2))

           (reverseRange [k1 k2] (.reverseRange ^TimestampedKeyValueStore @inner-store k1 k2))

           (all [] (.all ^TimestampedKeyValueStore @inner-store))

           (prefixScan [v1 v2] (.prefixScan ^TimestampedKeyValueStore @inner-store v1 v2))

           (put [k v] (.put ^TimestampedKeyValueStore @inner-store k v))

           (putIfAbsent [k v] (.putIfAbsent ^TimestampedKeyValueStore @inner-store k v))

           (^void putAll [^List kvs] (.putAll ^TimestampedKeyValueStore @inner-store kvs))

           (delete [k] (.delete ^TimestampedKeyValueStore @inner-store k))

           (approximateNumEntries [] (.approximateNumEntries ^TimestampedKeyValueStore @inner-store))

           (setFlushListener [listener, send-old-values] (.setFlushListener ^WrappedStateStore @inner-store listener send-old-values))

           (flushCache [] (.flushCache ^CachedStateStore @inner-store))

           (clearCache [] (.clearCache ^CachedStateStore @inner-store))

           (wrapped [] (do @inner-store)))

         (isa? (type @inner-store) MeteredKeyValueStore)
         (proxy [WrappedStateStore KeyValueStore CachedStateStore StateStore ReadOnlyKeyValueStore]
                [@inner-store]

           (delete [o] (.delete ^KeyValueStore @inner-store o))

           (^void put [o1 o2] (.put ^KeyValueStore @inner-store o1 o2))

           (^void putAll [^List kvs] (.putAll ^KeyValueStore @inner-store kvs))

           (putIfAbsent [o1 o2] (.putIfAbsent ^KeyValueStore @inner-store o1 o2))

           (^String name [] (.name ^StateStore @inner-store))

           (^void init [^StateStoreContext ctx ^StateStore s]
             (if (.isOpen ^StateStore @inner-store)
               (.register ctx this (reify StateRestoreCallback
                                     (restore [_this _a _b])))
               (.init ^KeyValueStore @inner-store ctx s)))

           (^void flush [] (.flush ^StateStore @inner-store))

           (^void close [] (.close ^StateStore @inner-store))

           (^boolean persistent [] (.persistent ^StateStore @inner-store))

           (^boolean isOpen [] (.isOpen ^StateStore @inner-store))

           (^QueryResult query [^Query query ^PositionBound position-bound ^QueryConfig query-config]
             (.query ^StateStore @inner-store query position-bound query-config))

           (^Position getPosition [] (.getPosition ^StateStore @inner-store))

           (get [o] (.get ^KeyValueStore @inner-store o))

           (^KeyValueIterator range [o1 o2] (.range ^KeyValueStore @inner-store o1 o2))

           (^KeyValueIterator reverseRange [o1 o2] (.reverseRange ^KeyValueStore @inner-store o1 o2))

           (^KeyValueIterator all [] (.all ^KeyValueStore @inner-store))

           (^KeyValueIterator reverseAll [] (.reverseAll ^KeyValueStore @inner-store))

           (^KeyValueIterator prefixScan [o ^Serializer serializer]
             (.prefixScan ^KeyValueStore @inner-store o serializer))

           (^long approximateNumEntries []
             (.approximateNumEntries ^KeyValueStore @inner-store))

           (setFlushListener
             [^CacheFlushListener listener b]
             (.setFlushListener ^MeteredKeyValueStore @inner-store listener b))

           (^void clearCache [] (.clearCache ^CachedStateStore @inner-store))

           (^void flushCache [] (.flushCache ^CachedStateStore @inner-store))

           (wrapped [] @inner-store))
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
  [^StoreBuilderWrapper store-factory store-name]
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
  (let [^StoreFactory alt (find-store-factory-alternative state-store-factory store-name)]
    (.withLoggingDisabled alt)))

(defn share-global-stores
  [topology]
  (let [i-builder (TopologyInternalsAccessor/internalTopologyBuilder topology)
        ^Map global-store-builders (.get global-state-builders-field i-builder)]
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
