package fun.messaging.distributed;

/**
 * Created by Kuldeep on 1/27/2015.
 */
public interface ValueWrapperFactory<T> {
    public ValueWrapper<T> newInstance(T value);
}
