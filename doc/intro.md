# Introduction to lazy_db_reader

TODO: write [great documentation](http://jacobian.org/writing/great-documentation/what-to-write/)

“”“
import java.util.List;
import java.util.function.Function;

interface TableReader {
    boolean hasMore();
    List<Object> loadMore();
    Object popOne();
}

class QueryState implements TableReader {
    private Object valueRef;
    private List<Object> cacheRef;
    private Function<Object, List<Object>> loadFn;

    public QueryState(Object valueRef, List<Object> cacheRef, Function<Object, List<Object>> loadFn) {
        this.valueRef = valueRef;
        this.cacheRef = cacheRef;
        this.loadFn = loadFn;
    }

    @Override
    public boolean hasMore() {
        List<Object> _cache = cacheRef.isEmpty() ? loadMore() : cacheRef;
        return !_cache.isEmpty();
    }

    @Override
    public List<Object> loadMore() {
        Function<Object, List<Object>> loadMoreFn = vRef -> {
            Object cValue = valueRef;
            List<Object> retList = loadFn.apply(cValue);
            valueRef = retList.get(retList.size() - 1);
            return retList;
        };
        if (cacheRef.isEmpty()) {
            synchronized (cacheRef) {
                cacheRef.addAll(loadMoreFn.apply(valueRef));
            }
        }
        return cacheRef;
    }

    @Override
    public Object popOne() {
        if (hasMore()) {
            synchronized (cacheRef) {
                Object retv = cacheRef.get(0);
                cacheRef.remove(0);
                return retv;
            }
        }
        return null;
    }
}

import java.util.stream.Stream;
import java.util.stream.Collectors;

public class LazyDbReader {
    public static Stream<Object> tableQuerySeq(QueryState queryState) {
        List<Object> batch = (List<Object>) queryState.popOne();
        if (batch != null) {
            return Stream.concat(batch.stream(), tableQuerySeq(queryState));
        }
        return Stream.empty();
    }
}
”“”

“”“
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import clojure.java.jdbc;

public class Sample {
    public static final Map<String, Object> conf = Map.of(
        "db-conf", Map.of(
            "subprotocol", "mysql",
            "subname", "",
            "user", "",
            "password", ""
        ),
        "fetch-size", 2000,
        "query-init-value", Map.of("mf_md5", ""),
        "write-batch-size", 1000,
        "htable-name", "install_summary_mf"
    );

    public static List<Map<String, Object>> myLoadFn(Map<String, Object> currentValue) {
        return jdbc.query(
            (Map<String, Object>) conf.get("db-conf"),
            "select pkg_name, mf_md5, cert_md5 from installed_apks where mf_md5>? order by mf_md5 asc limit ?",
            ((String) currentValue.get("mf_md5")),
            ((Integer) conf.get("fetch-size"))
        );
    }

    public static Put toPut(Map<String, Object> item) {
        byte[] rowKey = Bytes.toBytes(item.get("pkg_name") + "_" + item.get("mf_md5"));
        byte[] familyName = Bytes.toBytes("apk");
        byte[] qualifierName = Bytes.toBytes("cert_md5");
        byte[] value = Bytes.toBytes((String) item.get("cert_md5"));
        Put put = new Put(rowKey);
        put.add(familyName, qualifierName, value);
        return put;
    }

    public static void main(String[] args) {
        int batchSize = (int) conf.get("write-batch-size");
        System.out.println("in main...");
        QueryState queryState = new QueryState(new Ref(Map.of("mf_md5", "")), new Ref(new ArrayList<>()), Sample::myLoadFn);
        List<List<Map<String, Object>>> myTableSeq = tableQuerySeq(queryState);
        HTable table = new HTable(HBaseConfiguration.create(), (String) conf.get("htable-name"));
        myTableSeq.stream()
            .map(itemList -> itemList.stream().map(Sample::toPut).collect(Collectors.toList()))
            .forEach(puts -> table.put(puts));
    }
}

”“”
