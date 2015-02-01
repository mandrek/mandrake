package fun.messaging.distributed;

/**
 * Created by Kuldeep on 1/27/2015.
 */
public interface ValueWrapper<T> {
    void add(T val);
    T get();
}
