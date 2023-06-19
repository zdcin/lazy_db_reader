# Introduction to lazy_db_reader

TODO: write [great documentation](http://jacobian.org/writing/great-documentation/what-to-write/)

‘’‘
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
’‘’
