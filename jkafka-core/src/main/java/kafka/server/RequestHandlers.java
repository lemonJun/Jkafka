/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server;

import kafka.api.RequestKeys;
import kafka.common.annotations.ServerSide;
import kafka.log.LogManager;
import kafka.network.Receive;
import kafka.network.RequestHandler;
import kafka.network.RequestHandlerFactory;
import kafka.network.handlers.CreaterHandler;
import kafka.network.handlers.DeleterHandler;
import kafka.network.handlers.FetchHandler;
import kafka.network.handlers.MultiFetchHandler;
import kafka.network.handlers.MultiProduceHandler;
import kafka.network.handlers.OffsetsHandler;
import kafka.network.handlers.ProducerHandler;

/**
 * Request Handlers
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
@ServerSide
class RequestHandlers implements RequestHandlerFactory {

    private final FetchHandler fetchHandler;

    private final MultiFetchHandler multiFetchHandler;

    private final MultiProduceHandler multiProduceHandler;

    private final OffsetsHandler offsetsHandler;

    private final ProducerHandler producerHandler;

    private final CreaterHandler createrHandler;
    private final DeleterHandler deleterHandler;

    public RequestHandlers(LogManager logManager) {
        fetchHandler = new FetchHandler(logManager);
        multiFetchHandler = new MultiFetchHandler(logManager);
        multiProduceHandler = new MultiProduceHandler(logManager);
        offsetsHandler = new OffsetsHandler(logManager);
        producerHandler = new ProducerHandler(logManager);
        createrHandler = new CreaterHandler(logManager);
        deleterHandler = new DeleterHandler(logManager);
    }

    @Override
    public RequestHandler mapping(RequestKeys id, Receive request) {
        switch (id) {
            case FETCH:
                return fetchHandler;
            case PRODUCE:
                return producerHandler;
            case MULTIFETCH:
                return multiFetchHandler;
            case MULTIPRODUCE:
                return multiProduceHandler;
            case OFFSETS:
                return offsetsHandler;
            case CREATE:
                return createrHandler;
            case DELETE:
                return deleterHandler;
        }
        return null;
    }

}
