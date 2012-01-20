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

import java.nio.ByteBuffer;
import me.prettyprint.cassandra.serializers.*;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.cassandra.utils.StringUtils;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class UDFDelete extends GenericUDF {

  Cluster cluster;
  Keyspace ksp;
  ColumnFamilyTemplate cft;
  ColumnFamilyUpdater cfu;

  ObjectInspector[] argumentOI;
  String clusterName;
  String hostlist;
  String keyspace;
  String columnFamily;
  String rowKey;
  String column;

  public UDFDelete() {
    CFMetaData m;
    //m.c
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    this.argumentOI = arguments;
    if (!(arguments.length == 5 || arguments.length == 6)) {
      throw new UDFArgumentLengthException("This function takes 5 or 6 arguments but was "+arguments.length);
    }

    //for (int i = 0; i < 4; i++) {
    //  throw new UDFArgumentTypeException(i,
    //          "The argument of function should be primative" + ", but \""
    //          + arguments[i].getTypeName() + "\" is found");
    //}

    return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    clusterName = ((StringObjectInspector) argumentOI[0]).getPrimitiveJavaObject(arguments[0].get());
    hostlist = ((StringObjectInspector) argumentOI[1]).getPrimitiveJavaObject(arguments[1].get());
    keyspace = ((StringObjectInspector) argumentOI[2]).getPrimitiveJavaObject(arguments[2].get());
    columnFamily = ((StringObjectInspector) argumentOI[3]).getPrimitiveJavaObject(arguments[3].get());
    rowKey = ((StringObjectInspector) argumentOI[4]).getPrimitiveJavaObject(arguments[4].get());

    if (arguments.length == 6) {
      column = ((StringObjectInspector) argumentOI[5]).getPrimitiveJavaObject(arguments[5].get());
    }
    if (cluster == null) {
      cluster = HFactory.getOrCreateCluster(clusterName, hostlist);
      ksp = HFactory.createKeyspace(keyspace, cluster);
      cft = new ThriftColumnFamilyTemplate(ksp, this.columnFamily,
              BytesArraySerializer.get(),
              BytesArraySerializer.get());
    }
    //maybe try n times then throw exception? hector?
    if (column != null) {
      //cfu = cft.createUpdater(this.rowKey.getBytes());
      cfu.deleteColumn(column.getBytes());
      return 0;
    } else {
      try {
      //cft.deleteRow(this.rowKey.getBytes());
      ConnWrapper cw = new ConnWrapper("locahost",9154);
      ColumnPath cp = new ColumnPath();
      cp.setColumn_family(this.columnFamily);
      cw.getClient().remove(ByteBuffer.wrap(this.rowKey.getBytes()), null, System.nanoTime(), ConsistencyLevel.ONE);
      } catch (Exception ex){
        System.out.println("outtttttttttttttttttttttttttttttttttttttttttttttt");
        System.out.println(ex);
        ex.printStackTrace(System.out);
      }
      return 0;
    }
    
  }

  @Override
  public String getDisplayString(String[] strings) {
    return "strings";
  }
}
