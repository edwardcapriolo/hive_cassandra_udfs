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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class UDFDeleteTest extends HiveTestService {

  static EmbeddedCassandraService ecs;
  static Cluster cluster;
  static ColumnFamilyTemplate<String, String> data;
  static Keyspace ksp;

  public UDFDeleteTest() throws IOException {
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

    KeyspaceDefinition ksDef = HFactory.createKeyspaceDefinition("udfdelete");
    ColumnFamilyDefinition cfBak =
            HFactory.createColumnFamilyDefinition("udfdelete", "udfdelete",
            ComparatorType.UTF8TYPE);

    cluster.addKeyspace(ksDef);
    cluster.addColumnFamily(cfBak);

    ksp = HFactory.createKeyspace("udfdelete", cluster);
    data =
            new ThriftColumnFamilyTemplate<String, String>(ksp,
            "udfdelete",
            StringSerializer.get(),
            StringSerializer.get());

  }

  @Test
  public void testSingleThread() throws InterruptedException, 
          IOException, HiveServerException, TException {

    ColumnFamilyUpdater<String, String> dataUpdater = data.createUpdater("bob");
    dataUpdater.setString("lname", "smith");
    data.update(dataUpdater);

    dataUpdater = data.createUpdater("john");
    dataUpdater.setString("lname", "doloop");
    data.update(dataUpdater);

    dataUpdater = data.createUpdater("sara");
    dataUpdater.setString("lname", "connor");
    data.update(dataUpdater);

    Path p = new Path(this.ROOT_DIR, "afile");

    FSDataOutputStream o = this.getFileSystem().create(p);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(o));
    bw.write("john\t34\n");
    bw.write("sara\t33\n");
    bw.close();

    client.execute("create table atest (user string, age int)"+
            " row format delimited FIELDS TERMINATED BY '\\011'");
    client.execute("load data local inpath '" + p.toString()
            + "' into table atest");

    String jarFile;
    jarFile =com.jointhegrid.hivecasudfs.UDFDelete.class
            .getProtectionDomain().getCodeSource().getLocation().getFile();
    client.execute("add jar "+jarFile);
    
    jarFile=me.prettyprint.hector.api.Serializer.class
            .getProtectionDomain().getCodeSource().getLocation().getFile();
    client.execute("add jar "+jarFile);
    
    //jarFile=org.apache.cassandra.thrift.InvalidRequestException.class
      //      .getProtectionDomain().getCodeSource().getLocation().getFile();
    jarFile="/home/edward/.m2/repository/org/apache/cassandra/cassandra-thrift/1.0.6/cassandra-thrift-1.0.6.jar";
    client.execute("add jar "+jarFile);
    
    jarFile=com.google.common.collect.Iterables.class
            .getProtectionDomain().getCodeSource().getLocation().getFile();
    jarFile="/home/edward/.m2/repository/com/google/guava/guava/r09/guava-r09.jar";
    client.execute("add jar "+jarFile);
    /*
    jarFile=org.apache.cassandra.utils.Hex.class
            .getProtectionDomain().getCodeSource().getLocation().getFile();
    client.execute("add jar "+jarFile);
*/
    client.execute("CREATE TEMPORARY FUNCTION udfdelete "+
            " AS 'com.jointhegrid.hivecasudfs.UDFDelete'");
    client.execute("select user, "+
             "udfdelete('test cluster','localhost:9154','udfdelete','udfdelete',user) from atest");

    //client.execute("select user from atest");
    ColumnFamilyResult < String,String> looking = data.queryColumns("john");
    Assert.assertEquals(false,looking.hasResults());

    looking = data.queryColumns("sara");
    Assert.assertEquals(false,looking.hasResults());

    looking = data.queryColumns("bob");
    Assert.assertEquals(true,looking.hasResults());

  }
}
