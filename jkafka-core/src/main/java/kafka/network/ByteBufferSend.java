package kafka.network;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

import kafka.utils.NonThreadSafe;
import kafka.utils.Utils;

@NonThreadSafe
public class ByteBufferSend extends Send {
    public ByteBuffer buffer;

    public ByteBufferSend(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    private boolean complete;

    public ByteBufferSend(int size) {
        this(ByteBuffer.allocate(size));
    }

    @Override
    public int writeTo(GatheringByteChannel channel) {
        expectIncomplete();
        int written = 0;
        written += Utils.write(channel, buffer);
        if (!buffer.hasRemaining())
            complete = true;
        return written;
    }

    @Override
    public boolean complete() {
        return complete;
    }
}
