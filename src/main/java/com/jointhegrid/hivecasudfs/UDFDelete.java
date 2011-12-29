package com.jointhegrid.hivecasudfs;

import me.prettyprint.cassandra.serializers.*;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.cassandra.utils.StringUtils;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
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
      cfu = cft.createUpdater(this.rowKey.getBytes());
      cfu.deleteColumn(column.getBytes());
      return 0;
    } else {
      cft.deleteRow(this.rowKey.getBytes());
      return 0;
    }
    
  }

  @Override
  public String getDisplayString(String[] strings) {
    return "strings";
  }
}
