package im.ligas.kafka.stream;

import java.util.ArrayList;

public class FileData {
    private String id;
    private String absolutePath;
    private String fileName;
    private String tempLocation;  //Intermediate storage location of file. (S3, HDFS, etc) (* Need to fix on)
                                  //For simulation i am using the tempory location in local file system.
    private String source;
    private MetaData metaData;
    private ArrayList<Processor> processors= new ArrayList<>();
    private ArrayList<TargetSystem> destination= new ArrayList<>();

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public ArrayList<Processor> getProcessors() {
        return processors;
    }

    public void setProcessors(ArrayList<Processor> processors) {
        this.processors = processors;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAbsolutePath() {
        return absolutePath;
    }

    public void setAbsolutePath(String absolutePath) {
        this.absolutePath = absolutePath;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public MetaData getMetaData() {
        return metaData;
    }

    public void setMetaData(MetaData metaData) {
        this.metaData = metaData;
    }

    public ArrayList<TargetSystem> getDestination() {
        return destination;
    }

    public void setDestination(ArrayList<TargetSystem> destination) {
        this.destination = destination;
    }

    public void addDestination(TargetSystem targetSystem){
        this.destination.add(targetSystem);
    }

    public String getTempLocation() {
        return tempLocation;
    }

    public void setTempLocation(String tempLocation) {
        this.tempLocation = tempLocation;
    }
}
