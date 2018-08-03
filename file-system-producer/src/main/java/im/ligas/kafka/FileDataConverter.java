package im.ligas.kafka;

import im.ligas.kafka.client.FileData;
import im.ligas.kafka.client.MetaData;
import org.apache.kafka.connect.data.Struct;

public class FileDataConverter {

    public static FileData convertStructToFileData(Struct record){

        //Building MetaData object from MetaData Struct.
        MetaData.Builder metaDataBuilder= MetaData.newBuilder();
        Struct metaDataStruct= record.getStruct("metaData");
        MetaData metaData= metaDataBuilder.setUser(metaDataStruct.getString("user"))
                .setType(metaDataStruct.getString("type"))
                .setSize(metaDataStruct.getInt64("size"))
                .setCanRead(metaDataStruct.getBoolean("canRead"))
                .setCanWrite(metaDataStruct.getBoolean("canWrite"))
                .setCanExecute(metaDataStruct.getBoolean("canExecute"))
                .setCreated(metaDataStruct.getInt64("created"))
                .setLastAccessed(metaDataStruct.getInt64("lastAccessed"))
                .setLastModified(metaDataStruct.getInt64("lastModified"))
                .build();

        //Building FileData object from record Struct.
        FileData.Builder fileDataBuilder= FileData.newBuilder();
        FileData fileData= fileDataBuilder.setId(record.getString("id"))
                .setFileName(record.getString("fileName"))
                .setAbsolutePath(record.getString("absolutePath"))
                .setSource(record.getString("source"))
                .setTempLocation(record.getString("tempLocation"))
                .setMetaData(metaData)
                .build();

        return fileData;
    }
}
