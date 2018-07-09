package im.ligas.kafka;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

import static im.ligas.kafka.FileSystemSourceConnectorConfig.*;

public class FileSystemSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(FileSystemSourceTask.class);

    private File folder;
    private String topic = null;
    private int batchSize = FileSystemSourceConnectorConfig.DEFAULT_TASK_BATCH_SIZE;


    private Long streamOffset;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        String folderName = props.get(FOLDER_NAME_CONFIG);
        folder = new File(folderName);
        topic = props.get(TOPIC_CONFIG);
        batchSize = Integer.parseInt(props.get(TASK_BATCH_SIZE_CONFIG));
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        //TODO: Create SourceRecord objects that will be sent the kafka cluster.
        throw new UnsupportedOperationException("This has not been implemented.");
    }

    @Override
    public void stop() {
        //TODO: Do whatever is required to stop your task.
    }
}
