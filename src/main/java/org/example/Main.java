package org.example;
/*****/
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.notification.HookNotification.EntityCreateRequestV2;
import org.apache.atlas.model.notification.HookNotification;
//import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.notification.HookNotificationV1;
import org.apache.atlas.model.notification.HookNotification;

import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.hook.AtlasHook.getUser;
import static org.apache.atlas.hook.AtlasHook.notifyEntities;
import org.apache.atlas.hdfs.bridge.AtlasHDFSCreateEvent;


/***/

import org.apache.atlas.hdfs.bridge.AtlasHDFSHook;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.atlas.hook.AtlasHook;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;


public class Main extends AtlasHook{
    static FileSystem fileSystem = null;
    static Configuration config = null;

    public static void main(String[] args) throws IOException, MissingEventsException, InterruptedException {
        connectHdfs();

        /*try {
            createDirectory("/user/yexz/javaDir2");
        } catch (IOException exception){}
        try {
            writeFileToHDFS("write_hdfs_example.txt");
        } catch (IOException exception){}
        try {
            appendToHDFSFile("write_hdfs_example.txt");
        } catch (IOException exception){}
        try {
            readFileFromHDFS("write_hdfs_example.txt");
        } catch (IOException exception){}*/
        //checkExists();

        listMetadata();
        notif();
        //addAttr();
        //getAttr();

        // Start the hook
    }
    public static void connectHdfs(){
        config = new Configuration();
        config.set("fs.defaultFS", "hdfs://hadoopnamenode:9000");
    }
    public static void createDirectory(String pathToDir) throws IOException {
        fileSystem = FileSystem.get(config);
        String directoryName = pathToDir;
        Path path = new Path(directoryName);
        fileSystem.mkdirs(path);

    }
    public static void writeFileToHDFS(String fileName) throws IOException {
        FileSystem fileSystem = FileSystem.get(config);
        //Create a path
        Path hdfsWritePath = new Path("/user/yexz/javareadwriteexample/" + fileName);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath,true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
        bufferedWriter.write("Java API to write data in HDFS");
        bufferedWriter.newLine();
        bufferedWriter.close();
        fileSystem.close();
    }

    public static void appendToHDFSFile(String fileName) throws IOException {
        FileSystem fileSystem = FileSystem.get(config);
        //Create a path
        Path hdfsWritePath = new Path("/user/yexz/javareadwriteexample/" + fileName);

        FSDataOutputStream fsDataOutputStream = fileSystem.append(hdfsWritePath);

        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream,StandardCharsets.UTF_8));
        bufferedWriter.write("Java API to append data in HDFS file");
        bufferedWriter.newLine();
        bufferedWriter.close();
        fileSystem.close();
    } // tekhdmch

    public static void readFileFromHDFS(String fileName) throws IOException {

        FileSystem fileSystem = FileSystem.get(config);
        //Create a path
        Path hdfsReadPath = new Path("/user/yexz/javareadwriteexample/" + fileName);
        //Init input stream
        FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
        //Classical input stream usage
        String out= IOUtils.toString(inputStream, "UTF-8");
        System.out.println(out);
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        String line = null;
        while ((line=bufferedReader.readLine())!=null){
            System.out.println(line);
        }
        inputStream.close();
        fileSystem.close();
    }

    public static void checkExists() throws IOException {

        FileSystem fileSystem = FileSystem.get(config);
        String directoryName = "/user/yexz/javareadwriteexample/";
        Path path = new Path(directoryName);
        if(fileSystem.exists(path)){
            System.out.println("File/Folder Exists : "+path.getName());
        }else{
            System.out.println("File/Folder does not Exists : "+path.getName());
        }
    }

    public static void listMetadata() throws IOException{
        FileSystem fs = FileSystem.get(config);

// Path of the directory to list metadata for
        Path directoryPath = new Path("/");

// List all files and directories in the given directory
        FileStatus[] fileStatuses = fs.listStatus(directoryPath);

// Iterate over each file and directory and print its metadata
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println("File/directory name: " + fileStatus.getPath().getName());
            System.out.println("File/directory path: " + fileStatus.getPath());
            System.out.println("File/directory size: " + fileStatus.getLen());
            System.out.println("File/directory owner: " + fileStatus.getOwner());
            System.out.println("File/directory group: " + fileStatus.getGroup());
            System.out.println("File/directory permissions: " + fileStatus.getPermission());
            System.out.println("File/directory modification time: " + new Date(fileStatus.getModificationTime()));
            System.out.println("File/directory access time: " + new Date(fileStatus.getAccessTime()));
            System.out.println("Is directory? " + fileStatus.isDirectory());
            System.out.println("Is file? " + fileStatus.isFile());
            System.out.println();
        }

// Close the file system
        fs.close();
    }

    public static void notif() throws IOException, MissingEventsException, InterruptedException {

        fileSystem = FileSystem.get(config);
        HdfsAdmin admin = new HdfsAdmin(fileSystem.getUri(), config);
        DFSInotifyEventInputStream eventStream = admin.getInotifyEventStream();
        /**/
        List<HookNotification> messages = new ArrayList<>();
        /**/
        while (true) {
            EventBatch events = eventStream.take();
            for (Event event : events.getEvents()) {
                System.out.println("event type = " + event.getEventType());

                switch (event.getEventType()) {
                    case CREATE:
                        Event.CreateEvent createEvent = (Event.CreateEvent) event;
                        System.out.println("  path = " + createEvent.getPath());
                        List<Referenceable> entities = new ArrayList<>();
                        AtlasEntityDef entityDef = new AtlasEntityDef();

// set the qualified name for the entity class
                        entityDef.setName("");


                        AtlasEntity cre = new AtlasEntity(entityDef);
                        cre.setTypeName("DataSet");
                        cre.setAttribute("displayName","myCompany");
                        cre.setAttribute("owner","yexz");
                        cre.setAttribute("name","myCompany");
                        cre.setAttribute("qualifiedName","myCompany@DataSet");
                        //Referenceable cre = new Referenceable();
                        //cre.setTypeName("DataSet");
                        //entities.add(cre);
                        /**/




                        messages.add(new EntityCreateRequestV2("yexz", new AtlasEntity.AtlasEntitiesWithExtInfo(cre)));
                        //messages.add(new HookNotificationV1.EntityCreateRequest("yexz", entities));
                        notifyEntities(messages, null,10,null);
                        /**/
/**/



/**/
                        break;
                    case RENAME:
                        Event.RenameEvent renameEvent= (Event.RenameEvent) event;
                        System.out.println("  path = " + renameEvent.getDstPath());
                        break;
                    case UNLINK:
                        Event.UnlinkEvent unlinkEvent = (Event.UnlinkEvent) event;
                        System.out.println("  path = " + unlinkEvent.getPath());
                        break;

                    case APPEND:
                        Event.AppendEvent appendEvent = (Event.AppendEvent) event;
                        System.out.println("  path = " + appendEvent.getPath());

                    case METADATA:
                        assert event instanceof Event.MetadataUpdateEvent;
                        Event.MetadataUpdateEvent metadataUpdateEvent = (Event.MetadataUpdateEvent) event;
                        System.out.println("  path = " + metadataUpdateEvent.getPath());

                    default:
                        break;
                }
            }
        }


    }


    public static void addAttr() throws IOException {
        fileSystem = FileSystem.get(config);
        String filePath = "/user/yexz/test1";
        Path path = new Path(filePath);

        String name = "user.author";
        String value = "John Doe";

        fileSystem.setXAttr(path, name, value.getBytes());
    }
    public static void getAttr() throws IOException {
        fileSystem = FileSystem.get(config);
        String filePath = "/user/yexz/test1";
        Path path = new Path(filePath);

        String name = "user.author";
        String value = "John Doe";

        System.out.println(fileSystem.getXAttrs(path));
    }

    @Override
    public String getMessageSource() {
        return null;
    }




/*
    private List<Referenceable> createEntities(Event event, String user) {
        List<Referenceable> entities = new ArrayList<>();

        entities.add(FalconBridge.createClusterEntity((org.apache.falcon.entity.v0.cluster.Cluster) event.getEntity()));
        entities.add();

        //new AtlasHDFSCreateEvent();
        ent
            return entities;
        }*/
}