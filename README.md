# trident-cassandra

Cassandra state implementation for Twitter Storm Trident API.

All 3 state types are working good (non-transactional, opaque-transactional and transactional).

# Maven

https://clojars.org/trident-cassandra

```xml
<dependency>
  <groupId>trident-cassandra</groupId>
  <artifactId>trident-cassandra</artifactId>
  <!-- for storm 0.8.1 use wip1 version -->
  <version>0.0.1-wip2</version>
</dependency>
```

## Usage

Use static factory methods in `trident.cassandra.CassandraState`.

Word count topology sample:

```java
StateFactory cassandraStateFactory = CassandraState.transactional("localhost");

TridentState wordCounts = topology.newStream("spout1", spout)
                                  .each(new Fields("sentence"), new Split(), new Fields("word"))
                                  .groupBy(new Fields("word"))
                                  .persistentAggregate(cassandraStateFactory, new Count(), new Fields("count"))
                                  .parallelismHint(6);
```

CassandraState.Options, that could be passed to factory methods:

```java
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
```

## Bucket Usage

You can also use the bucket functionality of this trident extension. 
Essentially the base version persists against a set row key, so for each state you can only have a single row in a CF.

The extension in this fork allows you to specify a RowKeyStrategy, which can dynamically derive the row key for you. 

I have used this to implement rolling time windows through the concept of time buckets.

```java
		CassandraBucketState.BucketOptions options = new CassandraBucketState.BucketOptions();
		options.keyspace = "XXX";
		options.columnFamily = "XXX";
		options.rowKey = "XXX";
		options.keyStrategy = new TimeBasedRowStrategy(new ConcreteTimeProvider());
		return CassandraBucketState.nonTransactional("localhost", options);
```

This strategy simply appends a timestamp to the static row key:

```java
	public class TimeBasedRowStrategy implements RowKeyStrategy, Serializable {
		private TimeProvider timeProvider;
		public TimeBasedRowStrategy(TimeProvider timeProvider){
			this.timeProvider = timeProvider;
		}
		@Override
		public <T> String getRowKey(List<List<Object>> keys, Options<T> options) {
			return options.rowKey + StateUtils.formatHour(timeProvider.getTime());
		}

	}
```

The concrete time provider is really trivial:

```java
	public class ConcreteTimeProvider implements TimeProvider, Serializable {
	@Override
	public Date getTime() {
		return new Date(Time.currentTimeMillis());
	}

}
```

You could of course implement any kind of row key strategy... 

## License

Copyright (C) 2012 Sergey Lukjanov

Distributed under the Eclipse Public License, the same as Clojure.

(License is under discussions, it may be changed soon)
