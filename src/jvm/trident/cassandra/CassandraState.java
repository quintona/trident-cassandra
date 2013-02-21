package trident.cassandra;

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.spring.HectorTemplate;
import me.prettyprint.cassandra.service.spring.HectorTemplateImpl;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.SliceQuery;
import storm.trident.state.*;
import storm.trident.state.map.*;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author slukjanov
 */
public class CassandraState<T> implements IBackingMap<T> {

    private static final Map<StateType, Serializer> DEFAULT_SERIALZERS = Maps.newHashMap();

    static {
        DEFAULT_SERIALZERS.put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.OPAQUE, new JSONOpaqueSerializer());
    }

    public static class Options<T> implements Serializable {
        public int localCacheSize = 5000;
        public String globalKey = "$__GLOBAL_KEY__$";
        public Serializer<T> serializer = null;
        public String clusterName = "trident-state";
        public int replicationFactor = 1;
        public String keyspace = "test";
        public String columnFamily = "column_family";
        public String rowKey = "row_key";
    }

    public static StateFactory opaque(String hosts) {
        return opaque(hosts, new Options<OpaqueValue>());
    }

    public static StateFactory opaque(String hosts, Options<OpaqueValue> opts) {
        return new Factory(StateType.OPAQUE, hosts, opts);
    }

    public static StateFactory transactional(String hosts) {
        return transactional(hosts, new Options<TransactionalValue>());
    }

    public static StateFactory transactional(String hosts, Options<TransactionalValue> opts) {
        return new Factory(StateType.TRANSACTIONAL, hosts, opts);
    }

    public static StateFactory nonTransactional(String hosts) {
        return nonTransactional(hosts, new Options<Object>());
    }

    public static StateFactory nonTransactional(String hosts, Options<Object> opts) {
        return new Factory(StateType.NON_TRANSACTIONAL, hosts, opts);
    }

    protected static class Factory implements StateFactory {
        protected StateType stateType;
        protected Serializer serializer;
        protected String hosts;
        protected Options options;

        public Factory(StateType stateType, String hosts, Options options) {
            this.stateType = stateType;
            this.hosts = hosts;
            this.options = options;
            serializer = options.serializer;

            if (serializer == null) {
                serializer = DEFAULT_SERIALZERS.get(stateType);
            }

            if (serializer == null) {
                throw new RuntimeException("Serializer should be specified for type: " + stateType);
            }
        }
        
        public State makeState(Map conf, IMetricsContext metrics,
				int partitionIndex, int numPartitions) {
        	CassandraState state = new CassandraState(hosts, options, serializer);
        	return makeState(conf,metrics,partitionIndex,numPartitions,state);
        }

		@SuppressWarnings("rawtypes")
		protected State makeState(Map conf, IMetricsContext metrics,
				int partitionIndex, int numPartitions, CassandraState state) {

            CachedMap cachedMap = new CachedMap(state, options.localCacheSize);

            MapState mapState;
            if (stateType == StateType.NON_TRANSACTIONAL) {
                mapState = NonTransactionalMap.build(cachedMap);
            } else if (stateType == StateType.OPAQUE) {
                mapState = OpaqueMap.build(cachedMap);
            } else if (stateType == StateType.TRANSACTIONAL) {
                mapState = TransactionalMap.build(cachedMap);
            } else {
                throw new RuntimeException("Unknown state type: " + stateType);
            }

            return new SnapshottableMap(mapState, new Values(options.globalKey));
		}
    }

    protected HectorTemplate hectorTemplate;
    protected Options<T> options;
    protected Serializer<T> serializer;

    public CassandraState(String hosts, Options<T> options, Serializer<T> serializer) {
        hectorTemplate = new HectorTemplateImpl(
                HFactory.getOrCreateCluster(options.clusterName, new CassandraHostConfigurator(hosts)),
                options.keyspace, options.replicationFactor, "org.apache.cassandra.locator.SimpleStrategy",
                new ConfigurableConsistencyLevel()
        );

        this.options = options;
        this.serializer = serializer;
    }
    
    public List<T> multiGet(List<List<Object>> keys) {
    	return multiGet(keys, options.rowKey);
    }

    protected List<T> multiGet(List<List<Object>> keys, String rowKey) {
        Collection<Composite> columnNames = toColumnNames(keys);

        SliceQuery<String, Composite, byte[]> query = hectorTemplate.createSliceQuery(
                StringSerializer.get(),
                CompositeSerializer.get(),
                BytesArraySerializer.get())
                .setColumnFamily(options.columnFamily)
                .setKey(rowKey)
                .setColumnNames(columnNames.toArray(new Composite[columnNames.size()]));

        List<HColumn<Composite, byte[]>> result = query.execute().get().getColumns();

        Map<List<Object>, byte[]> resultMap = Maps.newHashMap();
        for (HColumn<Composite, byte[]> column : result) {
            Composite columnName = column.getName();
            List<Object> dimensions = Lists.newArrayListWithExpectedSize(columnName.size());
            for (int i = 0; i < columnName.size(); i++) {
                dimensions.add(columnName.get(i, StringSerializer.get()));
            }
            resultMap.put(dimensions, column.getValue());
        }

        List<T> values = Lists.newArrayListWithExpectedSize(keys.size());
        for (List<Object> key : keys) {
            byte[] bytes = resultMap.get(key);
            if (bytes != null) {
                values.add(serializer.deserialize(bytes));
            } else {
                values.add(null);
            }
        }

        return values;
    }
    
    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
    	multiPut(keys, values, options.rowKey);
    }

    protected void multiPut(List<List<Object>> keys, List<T> values, String rowKey) {
        Mutator<String> mutator = hectorTemplate.createMutator(StringSerializer.get());

        for (int i = 0; i < keys.size(); i++) {
            Composite columnName = toColumnName(keys.get(i));
            byte[] bytes = serializer.serialize(values.get(i));
            HColumn<Composite, byte[]> column = HFactory.createColumn(columnName, bytes);
            mutator.insert(rowKey, options.columnFamily, column);
        }

        mutator.execute();
    }

    protected Collection<Composite> toColumnNames(List<List<Object>> keys) {
        return Collections2.transform(keys, new Function<List<Object>, Composite>() {
            @Override
            public Composite apply(List<Object> key) {
                return toColumnName(key);
            }
        });
    }

    protected Composite toColumnName(List<Object> key) {
        Composite columnName = new Composite();
        for (Object component : key) {
            columnName.addComponent((String) component, StringSerializer.get());
        }

        return columnName;
    }
}
