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

public class UDFCasMapJoinTest extends HiveTestService {

  static EmbeddedCassandraService ecs;
  static Cluster cluster;
  static ColumnFamilyTemplate<String, String> data;
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
            ComparatorType.UTF8TYPE);

    cluster.addKeyspace(ksDef);
    cluster.addColumnFamily(cfBak);

    ksp = HFactory.createKeyspace("udfmj", cluster);
    data =
            new ThriftColumnFamilyTemplate<String, String>(ksp,
            "udfmj",
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

    client.execute("CREATE TEMPORARY FUNCTION udfcasmapjoin "+
            " AS 'com.jointhegrid.hivecasudfs.UDFCasMapJoin'");
    client.execute("select user, age, "+
             "udfcasmapjoin('test cluster','localhost:9154','udfmj','udfmj',user,'lname') as last_name from mjtest");
    String row = client.fetchOne();
    assertEquals("john\t34\tdoloop", row);
    //client.execute("drop table atest");

    

  }
}
