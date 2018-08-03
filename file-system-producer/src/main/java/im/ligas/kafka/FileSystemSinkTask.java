package im.ligas.kafka;


import im.ligas.kafka.client.FileData;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.util.Collection;
import java.util.Map;

import static im.ligas.kafka.FileSystemSinkConnectorConfig.FOLDER_NAME_CONFIG;

public class FileSystemSinkTask extends SinkTask {
    private static Logger log = LoggerFactory.getLogger(FileSystemSinkTask.class);

    private File folder;

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

            //Converting data from Struct type to FileData type.
            FileData fileData= FileDataConverter.convertStructToFileData((Struct) record.value());

            String tempLocation= fileData.getTempLocation() + File.separator + fileData.getId();

            log.trace("Processing message {} : {}", record.key(), fileData);
            try {
                //Copying file from temporary location to target location.
                FileUtils.copyFileToDirectory(new File(tempLocation), folder);

            } catch (Exception e) {
                log.warn("Could not copy file from temp location to target location");
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
