package kafka.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.api.ProducerRequest;
import kafka.api.RequestKeys;
import kafka.log.ILog;
import kafka.log.LogManager;
import kafka.message.ByteBufferMessageSet;
import kafka.message.CompressionCodec;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.mx.BrokerTopicStat;
import kafka.utils.Utils;

import java.util.Map;

import static java.lang.String.format;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 2014-11-14
 */
public class HttpRequestHandler {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    final String errorFormat = "Error processing %s on %s:%d";

    final LogManager logManager;

    public HttpRequestHandler(LogManager logManager) {
        this.logManager = logManager;
    }

    public void handle(Map<String, String> args, byte[] data) {
        RequestKeys requestKey = RequestKeys.valueOf(args.get("key"));
        ByteBufferMessageSet messageSet = new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, new Message(data));
        final String topic = args.get("topic");
        final int partition = Utils.getIntInRange(args, "partition", 0, 0, 1024);
        switch (requestKey) {
            case PRODUCE:
                produce(topic, partition, messageSet);
                break;
            default:
                break;
        }
    }

    private void produce(String topic, int partition, ByteBufferMessageSet messageSet) {
        final long st = System.currentTimeMillis();
        ProducerRequest request = new ProducerRequest(topic, partition, messageSet);
        if (logger.isDebugEnabled()) {
            logger.debug("Producer request " + request.toString());
        }
        handleProducerRequest(request);
        long et = System.currentTimeMillis();
        if (logger.isDebugEnabled()) {
            logger.debug("produce a message(set) cost " + (et - st) + " ms");
        }
    }

    protected void handleProducerRequest(ProducerRequest request) {
        int partition = request.getTranslatedPartition(logManager);
        try {
            final ILog log = logManager.getOrCreateLog(request.topic, partition);
            log.append(request.messages);
            long messageSize = request.messages.getSizeInBytes();
            if (logger.isDebugEnabled()) {
                logger.debug(messageSize + " bytes written to logs " + log);
                for (MessageAndOffset m : request.messages) {
                    logger.trace("wrote message " + m.offset + " to disk");
                }
            }
            BrokerTopicStat.getInstance(request.topic).recordBytesIn(messageSize);
            BrokerTopicStat.getBrokerAllTopicStat().recordBytesIn(messageSize);
        } catch (RuntimeException e) {
            if (logger.isDebugEnabled()) {
                logger.error(format(errorFormat, request.getRequestKey(), request.topic, request.partition), e);
            } else {
                logger.error("Producer failed. " + e.getMessage());
            }
            BrokerTopicStat.getInstance(request.topic).recordFailedProduceRequest();
            BrokerTopicStat.getBrokerAllTopicStat().recordFailedProduceRequest();
            throw e;
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.error(format(errorFormat, request.getRequestKey(), request.topic, request.partition), e);
            } else {
                logger.error("Producer failed. " + e.getMessage());
            }
            BrokerTopicStat.getInstance(request.topic).recordFailedProduceRequest();
            BrokerTopicStat.getBrokerAllTopicStat().recordFailedProduceRequest();
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
