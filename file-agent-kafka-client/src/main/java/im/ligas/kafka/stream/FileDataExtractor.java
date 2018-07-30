package im.ligas.kafka.stream;

import im.ligas.kafka.client.FileData;
import im.ligas.kafka.client.MetaData;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;

public class FileDataExtractor {

    public FileData getFileData(File file) {
        FileData.Builder fileDataBuilder = FileData.newBuilder();
        fileDataBuilder.setId(DigestUtils.sha1Hex(file.getAbsolutePath()));
        fileDataBuilder.setAbsolutePath(file.getAbsolutePath());
        fileDataBuilder.setFileName(file.getName());
        fileDataBuilder.setTempLocation("/tmp/intermediateStorage/");
        fileDataBuilder.setSource(System.getProperty("user.home"));

        try {
            BasicFileAttributes attrs = Files.readAttributes(file.toPath(), BasicFileAttributes.class);

            //Setting meta data of a file.
            MetaData.Builder metaDataBuilder = MetaData.newBuilder();
            metaDataBuilder.setCreated(attrs.creationTime().toMillis());
            metaDataBuilder.setLastModified(attrs.lastModifiedTime().toMillis());
            metaDataBuilder.setLastAccessed(attrs.lastAccessTime().toMillis());
            metaDataBuilder.setUser(System.getProperty("user.home").split(File.separator)[2]);
            metaDataBuilder.setCanRead(file.canRead());
            metaDataBuilder.setCanWrite(file.canWrite());
            metaDataBuilder.setCanExecute(file.canExecute());
            metaDataBuilder.setType(FilenameUtils.getExtension(file.getAbsolutePath()));
            metaDataBuilder.setSize(attrs.size());

            MetaData metaData= metaDataBuilder.build();
            fileDataBuilder.setMetaData(metaData);


        } catch (IOException e) {
            e.printStackTrace();
        }
        FileData fileData= fileDataBuilder.build();
        return fileData;
    }
}
