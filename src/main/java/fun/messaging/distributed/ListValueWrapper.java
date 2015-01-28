package fun.messaging.distributed;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Kuldeep on 1/27/2015.
 */
public class ListValueWrapper<T> implements ValueWrapper<T> {
    final List<T> values;

    ListValueWrapper(T value) {
        values = new ArrayList<>();
        values.add(value);
    }

    @Override
    public void add(T val) {
        if(values != null)
            values.add(val);
    }

    @Override
    public T get() {
        return null;
    }
}
