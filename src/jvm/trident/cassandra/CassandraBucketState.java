package trident.cassandra;

import java.util.List;
import java.util.Map;

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

import storm.trident.state.OpaqueValue;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;
import trident.cassandra.CassandraState.Factory;
import trident.cassandra.CassandraState.Options;

public class CassandraBucketState<T> extends CassandraState<T> {

	public CassandraBucketState(String hosts,
			BucketOptions<T> options,
			Serializer<T> serializer) {
		super(hosts, options, serializer);
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

	public static class BucketOptions<T> extends CassandraState.Options<T> {
        public RowKeyStrategy keyStrategy;
    }
	
	protected static class Factory extends CassandraState.Factory {

        public Factory(StateType stateType, String hosts, Options options) {
        	super(stateType, hosts, options);
        }

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public State makeState(Map conf, IMetricsContext metrics,
				int partitionIndex, int numPartitions) {
			CassandraBucketState state = new CassandraBucketState(hosts, (BucketOptions)options, serializer);
        	return makeState(conf,metrics,partitionIndex,numPartitions,state);
		}
    }
	
	private String getRowKey(List<List<Object>> keys){
		BucketOptions<T> opts = (BucketOptions<T>)super.options;
		return opts.keyStrategy.getRowKey(keys, super.options);
	}
	
	@Override
    public List<T> multiGet(List<List<Object>> keys) {
        return super.multiGet(keys, getRowKey(keys));
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
    	super.multiPut(keys, values, getRowKey(keys));
    }

}
