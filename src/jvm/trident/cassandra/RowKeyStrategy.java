package trident.cassandra;

import java.util.List;

import trident.cassandra.CassandraState.Options;

public interface RowKeyStrategy {

	<T> String getRowKey(List<List<Object>> keys, Options<T> options);

}
