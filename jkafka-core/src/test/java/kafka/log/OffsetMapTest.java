package kafka.log;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.junit.Test;

public class OffsetMapTest {
    @Test
    public void testBasicValidation() {
        validateMap(10);
        validateMap(100);
        validateMap(1000);
        validateMap(5000);
    }

    @Test
    public void testClear() {
        SkimpyOffsetMap map = new SkimpyOffsetMap(4000);
        for (int i = 0; i < 10; ++i)
            map.put(key(i), i);

        for (int i = 0; i < 10; ++i)
            assertEquals((long) i, map.get(key(i)));

        map.clear();

        for (int i = 0; i < 10; ++i)
            assertEquals(map.get(key(i)), -1L);
    }

    public ByteBuffer key(int key) {
        return ByteBuffer.wrap((key + "").getBytes());
    }

    public SkimpyOffsetMap validateMap(int items) {
        return validateMap(items, 0.5);
    }

    public SkimpyOffsetMap validateMap(int items, double loadFactor /*= 0.5*/) {
        SkimpyOffsetMap map = new SkimpyOffsetMap((int) (items / loadFactor * 24));
        for (int i = 0; i < items; ++i)
            map.put(key(i), i);
        int misses = 0;
        for (int i = 0; i < items; ++i)
            assertEquals(map.get(key(i)), (long) i);
        return map;
    }

}
