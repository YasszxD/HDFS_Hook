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

import java.util.*;

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


public class Main extends AtlasHook{
    static FileSystem fileSystem = null;
    static Configuration config = null;

    public static void main(String[] args) throws IOException, MissingEventsException, InterruptedException {
        HdfsY hdfsY = new HdfsY();
        config = HdfsY.config;
        fileSystem = FileSystem.get(config);

        aVoid();

        // Start the hook
        //notif("csv_files");
    }


    public static void aVoid(){
        List<HookNotification> messages = new ArrayList<>();
        AtlasEntity cre2 = new AtlasEntity();
        cre2.setTypeName("renameProcess");

        List<AtlasObjectId> objectIds1 = new ArrayList<>();

        List<AtlasObjectId> objectIds = new ArrayList<>();


        cre2.setAttribute("qualifiedName","p1@renameProcess");
        cre2.setAttribute("name","p1");
        //cre2.setAttribute("inputs",);
        AtlasEntity cre0 = new AtlasEntity();
        AtlasEntity cre1 = new AtlasEntity();
        AtlasObjectId atlasObjectId = new AtlasObjectId();
        atlasObjectId.setGuid("c9e9201c-0fe2-4e86-b3f7-84fb1deb12c3");
        atlasObjectId.setTypeName("csv_files");
        AtlasObjectId atlasObjectId1 = new AtlasObjectId();
        atlasObjectId1.setGuid("b430e1fc-9a47-46b5-a101-31b8f5891de7");
        atlasObjectId1.setTypeName("csv_files");
        objectIds.add(atlasObjectId);
        objectIds1.add(atlasObjectId1);
        cre2.setAttribute("inputs",objectIds1);
        cre2.setAttribute("outputs",objectIds);

        messages.add(new EntityCreateRequestV2("yexz", new AtlasEntity.AtlasEntitiesWithExtInfo(cre2)));
        notifyEntities(messages, null,5,null);
    }


    public static void notif(String TypeName) throws IOException, MissingEventsException, InterruptedException {
        int i=0;
        //fileSystem = FileSystem.get(config);
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



                        AtlasEntityDef entityDef = new AtlasEntityDef();
                        entityDef.setName(createEvent.getPath());
                        AtlasEntity cre = new AtlasEntity(entityDef);
                        cre.setTypeName(TypeName);
                        Path path = new Path(createEvent.getPath());
                        cre.setAttribute("owner",fileSystem.getFileStatus(path).getOwner());
                        cre.setAttribute("path",fileSystem.getFileStatus(path).getPath());
                        cre.setAttribute("ModificationTime",fileSystem.getFileStatus(path).getModificationTime());

                        Map<String, byte[]> attrs = HdfsY.getAttr(createEvent.getPath());

                        for (Map.Entry<String, byte[]> entry : attrs.entrySet()) {
                            String xAttrName = entry.getKey().substring(5);
                            byte[] xAttrValue = entry.getValue();
                            String encodedValue = new String(xAttrValue, StandardCharsets.UTF_8);
                            cre.setAttribute(xAttrName,encodedValue);

                        }


                        messages.add(new EntityCreateRequestV2("yexz", new AtlasEntity.AtlasEntitiesWithExtInfo(cre)));
                        notifyEntities(messages, null,5,null);
                        /**/
                        break;
                    case RENAME:
                        Event.RenameEvent renameEvent= (Event.RenameEvent) event;
                        System.out.println("  path = " + renameEvent.getDstPath());


                        AtlasEntity cre0 = new AtlasEntity();
                        cre0.setAttribute("name",renameEvent.getSrcPath().split("/")[3]);

                        AtlasEntity cre1 = new AtlasEntity();
                        cre1.setTypeName(TypeName);
                        String qualifiedName = renameEvent.getSrcPath().split("/")[3]+"@csv_files";
                        System.out.println("****"+qualifiedName);
                        cre1.setAttribute("qualifiedName",qualifiedName);
                        cre1.setAttribute("name",renameEvent.getDstPath().split("/")[3]);


                        AtlasEntity cre2 = new AtlasEntity();
                        cre2.setTypeName("renameProcess");
                        //cre2.setAttribute("inputs",);




                        messages.add(new EntityCreateRequestV2("yexz", new AtlasEntity.AtlasEntitiesWithExtInfo(cre1)));
                        notifyEntities(messages, null,5,null);

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


    @Override
    public String getMessageSource() {
        return null;
    }


}