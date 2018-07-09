package im.ligas.kafka.stream;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;

public class FileDataExtractor {
    public FileData getFileData(File file) {
        FileData fileData = new FileData();
        fileData.setAbsolutePath(file.getAbsolutePath());
        fileData.setFileName(file.getName());

        try {
            BasicFileAttributes attrs = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
            fileData.setCreated(attrs.creationTime().toMillis());
            fileData.setSize(attrs.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileData;
    }
}
