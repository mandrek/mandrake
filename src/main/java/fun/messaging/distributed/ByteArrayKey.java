package fun.messaging.distributed;

import java.util.Arrays;

/**
 * Created by kuldeep on 1/24/15.
 */
public class ByteArrayKey {
    private byte[] key;

    public ByteArrayKey(byte[] key) {
        this.key = key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ByteArrayKey that = (ByteArrayKey) o;
        if (!Arrays.equals(key, that.key)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return key != null ? Arrays.hashCode(key) : 0;
    }
}
