package spring.batch.springbatchexample.part3;

import org.springframework.batch.item.ItemProcessor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class DuplicateValidationProcessor<T> implements ItemProcessor<T, T> {

    private final Map<String, Object> keyPool = new ConcurrentHashMap<>();
    private final Function<T, String> keyExtractor;
    private final boolean allowDuplicate;

    public DuplicateValidationProcessor(Function<T, String> keyExtractor, boolean allowDuplicate) {
        this.keyExtractor = keyExtractor;
        this.allowDuplicate = allowDuplicate;
    }

    @Override
    public T process(T item) throws Exception {
        if (allowDuplicate) { // 필터링을 하지 않겠다
            return item;
        }

        String key = keyExtractor.apply(item); // key 추출
        if (keyPool.containsKey(key)) { // key 가 있으면 중복
            return null;
        }

        keyPool.put(key, key);
        return item;
    }
}
