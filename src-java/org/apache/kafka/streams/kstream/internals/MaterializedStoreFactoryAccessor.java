//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.kafka.streams.kstream.internals;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.StateStore;

public abstract class MaterializedStoreFactoryAccessor<K, V, S extends StateStore> extends MaterializedStoreFactory<K, V, S> {

    public MaterializedStoreFactoryAccessor(MaterializedInternal<K, V, S> materialized) {
        super(materialized);
    }

    public static <K> Serde<K> keySerde(MaterializedStoreFactory<K, ?, ?> factory){
        return factory.materialized.keySerde();
    }

    public static <V> Serde<V> valueSerde(MaterializedStoreFactory<?, V, ?> factory){
        return factory.materialized.valueSerde();
    }

}
