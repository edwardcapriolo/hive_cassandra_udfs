/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jointhegrid.hivecasudfs;

import com.jointhegrid.hive_test.HiveTestService;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Assert;

import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

/**
 *
 * @author edward
 */

public class UDFCompositeToByteArrayTest  {

  static EmbeddedCassandraService ecs;
  static Cluster cluster;
  static ColumnFamilyTemplate<ByteBuffer, ByteBuffer> data;
  static Keyspace ksp;

  public UDFCompositeToByteArrayTest() throws IOException {
    super();
  }

  public void setUp() throws Exception {
    /*
    super.setUp();
    CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
    cleaner.prepare();

    ecs = new EmbeddedCassandraService();
    ecs.start();
    cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9154");

    KeyspaceDefinition ksDef = HFactory.createKeyspaceDefinition("comptest");
    cluster.addKeyspace(ksDef);
    StringSerializer se = new StringSerializer();
    LongSerializer le = new LongSerializer();
    CqlQuery<String,String,Long> cqlQuery
            = new CqlQuery<String,String,Long>(ksp, se, se, le);
    cqlQuery.setQuery("select * from StandardLong1");
    */
  }

  @Test
  public void testShallow() throws Exception {
    List<byte[]> b = new ArrayList<byte[]>();
    b.add( "a".getBytes());
    b.add( "b".getBytes() );
    b.add( "wombat".getBytes() );
    byte[] composite = CompositeTool.makeComposite(b);
    BytesWritable bw = new BytesWritable();
    bw.set(composite, 0, composite.length);

    UDFCompositeToByteArray j = new UDFCompositeToByteArray();
    List<byte[]> back = j.evalInternal(bw);
    Assert.assertEquals(b.size(), back.size());
    for (int i=0;i<b.size();i++){
      Assert.assertEquals(new String(b.get(i)), new String(back.get(i)));
    }
  }
}
