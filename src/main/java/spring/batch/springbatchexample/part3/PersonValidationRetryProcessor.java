package spring.batch.springbatchexample.part3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.retry.support.RetryTemplateBuilder;

@Slf4j
public class PersonValidationRetryProcessor implements ItemProcessor<Person, Person> {

    private final RetryTemplate retryTemplate;

    public PersonValidationRetryProcessor() {
        this.retryTemplate = new RetryTemplateBuilder()
                .maxAttempts(3)
                .retryOn(NotFoundNameException.class) // NotFoundNameException 에러를 3번 발생할 때 까지 허용하고 재시도
                .withListener(new SavePersonRetryListener())
                .build();

    }

    @Override
    public Person process(Person item) throws Exception {
        return this.retryTemplate.execute(context -> {
            // RetryCallback -- maxAttempts 인자값인 3번 RetryCallback이 실행됨
            if (item.isNotEmptyName()) {
                return item;
            }
            throw new NotFoundNameException();
        }, context -> {
            // RecoveryCallback -- 3번 RetryCallback이 실행된 후 RecoveryCallback 실행됨
            return item.unknowName();
        });
    }

    public static class SavePersonRetryListener implements RetryListener {

        @Override
        public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
            return true; // true 일 때 적용 가능
        }

        @Override
        public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
            log.info("close");
        }

        @Override
        // retryTemplate에 정의된 오류가 throw 되었을 때 호출됨
        public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
            log.info("onError");
        }
    }
}
