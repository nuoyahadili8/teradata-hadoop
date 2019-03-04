package com.teradata.connector.hdfs.serde;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.hdfs.utils.HdfsTextTransform;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

public class HdfsTextSerDeTest {
    @Test
    public void testSerialize() throws Exception {
        final HdfsTextSerDe serde = new HdfsTextSerDe();
        serde.targetNullString = "";
        serde.targetNullNonString = "";
        serde.targetSeparator = "|";
        serde.outValue = new Text();
        final ConnectorRecord record = new ConnectorRecord(5);
        record.set(0, "unit test'ing'");
        record.set(1, 7);
        record.set(2, false);
        record.set(3, "");
        record.set(4, "");
        final Writable writable = serde.serialize(record);
        Assert.assertEquals((Object) writable, (Object) new Text("unit test'ing'|7|false||"));
    }

    @Test
    public void testDeserialize() throws Exception {
        final HdfsTextSerDe serde = new HdfsTextSerDe();
        serde.sourceEscapedByString = "";
        serde.sourceEnclosedByString = "";
        serde.sourceSeparator = "\\|";
        serde.sourceNullString = "";
        serde.sourceTransformer = new HdfsTextTransform[]{new HdfsTextTransform.VarcharTransform(), new HdfsTextTransform.IntTransform(), new HdfsTextTransform.BooleanTransform(), new HdfsTextTransform.VarcharTransform(), new HdfsTextTransform.VarcharTransform()};
        final ConnectorRecord testRecord = serde.deserialize((Writable) new Text("unit test'ing'|7|false||"));
        Assert.assertEquals((long) testRecord.getAllObject().length, 5L);
        Assert.assertEquals(testRecord.get(0), (Object) "unit test'ing'");
        Assert.assertEquals(testRecord.get(1), (Object) 7);
        Assert.assertEquals(testRecord.get(2), (Object) false);
        Assert.assertEquals(testRecord.get(3), (Object) null);
        Assert.assertEquals(testRecord.get(4), (Object) null);
    }
}
