package im.ligas.kafka.stream;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;

public class FileDataExtractor {


    public FileData getFileData(File file) {
        FileData fileData = new FileData();
        fileData.setAbsolutePath(file.getAbsolutePath());
        fileData.setId(DigestUtils.sha1Hex(fileData.getAbsolutePath()));
        fileData.setFileName(file.getName());

        fileData.setSource(System.getProperty("user.home")); //What is the source system we should set to? IPaddress?

        fileData.setTempLocation("/tmp/intermediate-data"); //File is stored with file-id in this temp location.

        //Note: We need to fix on how we get the target system and properties of target system.
        //For now i have made the default configurations.

        //Setting the target system configurations.
        ArrayList<TargetSystem> targetSystemList= new ArrayList<>();
        TargetSystem targetSystem= new TargetSystem();
        targetSystem.setTargetSystem("target System"); //Is it going to be url of the target system.
        //Need to define the properties
        targetSystem.setProps(new HashMap<>());     //What are the properties we need specify here?
        targetSystemList.add(targetSystem);
        fileData.setDestination(targetSystemList);

        try {
            BasicFileAttributes attrs = Files.readAttributes(file.toPath(), BasicFileAttributes.class);

            //Setting meta data of a file.
            MetaData metaData= new MetaData();
            metaData.setCreatedTime(attrs.creationTime().toMillis());
            metaData.setModifiedTime(attrs.lastModifiedTime().toMillis());
            metaData.setSize(attrs.size());
            metaData.setLastAccessed(attrs.lastAccessTime().toMillis());
            metaData.setUser(System.getProperty("user.home").split(File.separator)[2]);
            metaData.setCanRead(file.canRead());
            metaData.setCanWrite(file.canWrite());
            metaData.setCanExecute(file.canExecute());
            metaData.setFileType(FilenameUtils.getExtension(file.getAbsolutePath()));
            fileData.setMetaData(metaData);

        } catch (IOException e) {
            e.printStackTrace();
        }

        //Note: For now i had simply set some default processor with no properties.
        //We need to fix on processors and their functionality and how user can give their custom processor and properties.

        //Set the processors to be applied on file.
        ArrayList<Processor> processors= new ArrayList<>();
        Processor processor= new Processor();
        //Setting the default processor with no properties.
        processor.setName("processor1");
        processor.setProps(new HashMap<>());
        processors.add(processor);
        fileData.setProcessors(processors);

        return fileData;
    }
}
