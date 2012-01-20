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
import java.nio.charset.CharacterCodingException;
import java.util.logging.Level;
import java.util.logging.Logger;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.BytesWritable;


public class UDFCasMapJoin extends GenericUDF{

  Cluster cluster;
  Keyspace ksp;
  ColumnFamilyTemplate cft;
  ColumnFamilyUpdater cfu;

  ObjectInspector[] argumentOI;
  String clusterName;
  String hostlist;
  String keyspace;
  String columnFamily;
  ByteBuffer rowKey;
  ByteBuffer column;

  public UDFCasMapJoin() {
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    this.argumentOI = arguments;
    if (!(arguments.length == 5 || arguments.length == 6)) {
      throw new UDFArgumentLengthException("This function takes 5 or 6 arguments but was "+arguments.length);
    }

    //return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    clusterName = ((StringObjectInspector) argumentOI[0]).getPrimitiveJavaObject(arguments[0].get());
    hostlist = ((StringObjectInspector) argumentOI[1]).getPrimitiveJavaObject(arguments[1].get());
    keyspace = ((StringObjectInspector) argumentOI[2]).getPrimitiveJavaObject(arguments[2].get());
    columnFamily = ((StringObjectInspector) argumentOI[3]).getPrimitiveJavaObject(arguments[3].get());
    BytesWritable aRowKey = ((BinaryObjectInspector) argumentOI[4]).getPrimitiveWritableObject(arguments[4].get());
    BytesWritable aColumn = ((BinaryObjectInspector) argumentOI[5]).getPrimitiveWritableObject(arguments[5].get());

    byte [] rk = new byte [aRowKey.getLength()];
    for (int i =0;i<rk.length;i++){
      rk[i]=aRowKey.getBytes()[i];
    }
    byte [] cl = new byte [aColumn.getLength()];
    for (int i =0;i<cl.length;i++){
      cl[i]=aColumn.getBytes()[i];
    }
    rowKey = ByteBuffer.wrap(rk);
    //column = ByteBuffer.wrap(aColumn.getBytes());
    //column = ByteBuffer.wrap( "lname".getBytes() );
    System.out.println(cl.length);
    System.out.println(new String(cl));
    column = ByteBuffer.wrap( cl );

    if (cluster == null) {
      cluster = HFactory.getOrCreateCluster(clusterName, hostlist);
      ksp = HFactory.createKeyspace(keyspace, cluster);
      cft = new ThriftColumnFamilyTemplate(ksp, this.columnFamily,
              ByteBufferSerializer.get(),
              ByteBufferSerializer.get());
    }
    try {
      System.out.println("InUDF----rk" + ByteBufferUtil.string(rowKey));
    } catch (CharacterCodingException ex) {
      Logger.getLogger(UDFCasMapJoin.class.getName()).log(Level.SEVERE, null, ex);
    }
    ColumnFamilyResult<ByteBuffer,ByteBuffer> res =cft.queryColumns(rowKey);
    byte []  columnRes = res.getByteArray(column);
    System.out.println("InUDF----cr" + new String(columnRes));
    ByteArrayRef bar = new ByteArrayRef();
    bar.setData(columnRes);
    return bar;
    
  }

  @Override
  public String getDisplayString(String[] strings) {
    return "mapjoin";
  }
}

