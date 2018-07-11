package im.ligas.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static im.ligas.kafka.FileSystemSinkConnectorConfig.FOLDER_NAME_CONFIG;

public class FileSystemSinkTask extends SinkTask {
    private static Logger log = LoggerFactory.getLogger(FileSystemSinkTask.class);

    private File folder;
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        String folderName = props.get(FOLDER_NAME_CONFIG);
        if (folderName == null) {
            throw new ConnectException("Missing target folder!");
        }
        log.info("Folder name {}", folderName);
        folder = new File(folderName);
        if (!folder.exists()) {
            if (!folder.mkdirs()) {
                throw new ConnectException("Unable to create target folder");
            }
        } else {
            if (!folder.isDirectory()) {
                throw new ConnectException(folderName + " is not a folder");
            }
        }
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord record : sinkRecords) {
            HashMap value = (HashMap) record.value();
            log.trace("Processing message {} : {}", record.key(), value);
            try {
                String absolutePath = (String) value.get("absolutePath");
                String fileName = (String) value.get("fileName");

                FileUtils.copyFileToDirectory(new File(absolutePath), folder);

                File metadata = new File(folder, fileName + ".json");
                mapper.writeValue(metadata, value);
            } catch (JsonProcessingException e) {
                log.warn("Could not parse message {}", value);
            } catch (IOException e) {
                log.warn("Unable to copy file {}", value);
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        //nothing
    }

    @Override
    public void stop() {
        //nothing
    }

}
