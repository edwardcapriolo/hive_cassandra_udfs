/*
Copyright 2011 Edward Capriolo

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.jointhegrid.hivecasudfs;

import com.jointhegrid.hive_test.HiveTestService;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class UDFCasMapJoinTest extends HiveTestService {

  static EmbeddedCassandraService ecs;
  static Cluster cluster;
  static ColumnFamilyTemplate<ByteBuffer, ByteBuffer> data;
  static Keyspace ksp;

  public UDFCasMapJoinTest() throws IOException {
    super();
  }

  //@BeforeClass
  public void setUp() throws Exception {
    super.setUp();
    CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
    cleaner.prepare();

    ecs = new EmbeddedCassandraService();
    ecs.start();
    cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9154");

    KeyspaceDefinition ksDef = HFactory.createKeyspaceDefinition("udfmj");
    ColumnFamilyDefinition cfBak =
            HFactory.createColumnFamilyDefinition("udfmj", "udfmj",
            ComparatorType.BYTESTYPE);

    cluster.addKeyspace(ksDef);
    cluster.addColumnFamily(cfBak);

    ksp = HFactory.createKeyspace("udfmj", cluster);
    data =
            new ThriftColumnFamilyTemplate<ByteBuffer, ByteBuffer>(ksp,
            "udfmj",
            ByteBufferSerializer.get(),
            ByteBufferSerializer.get());

  }

  @Test
  public void testSingleThread() throws InterruptedException,
          IOException, HiveServerException, TException {

    ColumnFamilyUpdater<ByteBuffer, ByteBuffer> dataUpdater
            = data.createUpdater(ByteBufferUtil.bytes("bob"));
    dataUpdater.setByteBuffer(ByteBufferUtil.bytes("lname"),ByteBufferUtil.bytes("smith") );
    data.update(dataUpdater);

    dataUpdater = data.createUpdater(ByteBufferUtil.bytes("john"));
    dataUpdater.setByteBuffer(ByteBufferUtil.bytes("lname"), ByteBufferUtil.bytes("doloop"));
    data.update(dataUpdater);

    dataUpdater = data.createUpdater(ByteBufferUtil.bytes("sara"));
    dataUpdater.setByteBuffer(ByteBufferUtil.bytes("lname"), ByteBufferUtil.bytes("connor"));
    data.update(dataUpdater);

    Path p = new Path(this.ROOT_DIR, "afile");

    FSDataOutputStream o = this.getFileSystem().create(p);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(o));
    bw.write("john\t34\n");
    bw.write("sara\t33\n");
    bw.close();

    client.execute("create table mjtest (user string, age int)"+
            " row format delimited FIELDS TERMINATED BY '\\011'");
    client.execute("load data local inpath '" + p.toString()
            + "' into table mjtest");

    String jarFile;
    jarFile =com.jointhegrid.hivecasudfs.UDFDelete.class
            .getProtectionDomain().getCodeSource().getLocation().getFile();
    client.execute("add jar "+jarFile);
    jarFile=me.prettyprint.hector.api.Serializer.class
            .getProtectionDomain().getCodeSource().getLocation().getFile();
    client.execute("add jar "+jarFile);
    jarFile=org.apache.cassandra.thrift.InvalidRequestException.class
            .getProtectionDomain().getCodeSource().getLocation().getFile();
    client.execute("add jar "+jarFile);
    jarFile=com.google.common.collect.Iterables.class
            .getProtectionDomain().getCodeSource().getLocation().getFile();
    client.execute("add jar "+jarFile);
    jarFile= org.apache.cassandra.utils.ByteBufferUtil.class
            .getProtectionDomain().getCodeSource().getLocation().getFile();
    client.execute("add jar "+jarFile);
    jarFile= org.apache.commons.lang.ArrayUtils.class.
            getProtectionDomain().getCodeSource().getLocation().getFile();
    client.execute("add jar "+jarFile);

    client.execute("CREATE TEMPORARY FUNCTION udfcasmapjoin "+
            " AS 'com.jointhegrid.hivecasudfs.UDFCasMapJoin'");
    //constant as string yuk
    client.execute("select user, age, "+
             "udfcasmapjoin('test cluster','localhost:9154','udfmj','udfmj', "
             + "CAST(user as binary),  CAST( CONCAT(substring(user,0,0),'lname') as binary)  ) as last_name from mjtest");
    String row = client.fetchOne();
    assertEquals("john\t34\tdoloop", row);
    //client.execute("drop table atest");

    
  }
}
