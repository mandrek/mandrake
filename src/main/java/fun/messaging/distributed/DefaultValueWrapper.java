package fun.messaging.distributed;

/**
 * Created by Kuldeep on 1/27/2015.
 */
public class DefaultValueWrapper<T> implements ValueWrapper<T> {
    private T value;
    DefaultValueWrapper(T val) {
        this.value = val;
    }

    @Override
    public void add(T val) {
        this.value = val;
    }

    @Override
    public T get() {
        return this.value;
    }
}
