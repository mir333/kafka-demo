/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package im.ligas.kafka.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.Properties;

/**
 * In this example, we implement a simple LineSplit program using the high-level Streams DSL
 * that reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text,
 * and writes the messages as-is into a sink topic "streams-pipe-output".
 */
public class FileDataReader {

    private static Logger LOG = LoggerFactory.getLogger(FileDataReader.class);

    private final static String APP_ID = "file-data-agent-local-fs";
    private final static String TOPIC = "basic-file-data";
    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092";

    private static FileDataExtractor fileDataExtractor = new FileDataExtractor();

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new ConfigException("wrong input data. Please provide a full path to target folder.");
        }

        String folderName = args[0];
        File file = new File(folderName);
        if (!file.isDirectory()) {
            throw new ConfigException("Not a directory. Please provide a full path to target folder.");
        }

        Collection<File> files = FileUtils.listFiles(file, null, true);

        runProducer(files);
    }

    static void runProducer(Collection<File> files) throws InterruptedException {

        final Producer<String, JsonNode> producer = createProducer();
        long time = System.currentTimeMillis();
        try {
            files.forEach(file -> {
                String absolutePath = file.getAbsolutePath();
                String key = DigestUtils.sha1Hex(absolutePath);
                FileData data = fileDataExtractor.getFileData(file);
                LOG.debug("processing file {}", key);

                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode dataNode = objectMapper.valueToTree(data);

                final ProducerRecord<String, JsonNode> record = new ProducerRecord<>(TOPIC, key, dataNode);
                producer.send(record, (metadata, exception) -> {
                    long elapsedTime = System.currentTimeMillis() - time;
                    if (metadata != null) {
                        LOG.debug("sent record(key={} value={})  meta(partition={}, offset={}) time={}",
                                record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
                    } else {
                        LOG.warn("Could not send message", exception);
                    }
                });
            });
        } finally {
            producer.flush();
            producer.close();
        }
    }

    private static Producer<String, JsonNode> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, APP_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
