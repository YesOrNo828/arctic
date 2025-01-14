/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.arctic.spark.hive;

import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.UnkeyedTable;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.util.StructLikeMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMigrateHiveTable extends SparkHiveTestContext {

  private final String sourceDatabase = "db1" ;
  private final String sourceTable = "hive_table";
  private final String database = "arctic_db";
  private final String table = "arctic_table";

  @BeforeClass
  public static void beforeAll() throws IOException {
    setUpTestDirAndArctic();
    setUpHMS();
    setUpSparkSession();
  }

  @AfterClass
  public static void tearDown() {
    cleanUpHive();
    cleanUpAms();
    cleanUpSparkSession();
  }

  @Before
  public void setUpArcticDatabase(){
    sql("use " + catalogName);
    sql("create database if not exists " + database);
  }

  @After
  public void cleanUpAllTables(){
    sql("drop table {0}.{1}.{2}", catalogName, database, table);
    sql("drop table {0}.{1}.{2}", "spark_catalog", sourceDatabase, sourceTable);
  }

  @Test
  public void testMigrateHiveTable() {
    sql("use spark_catalog");
    sql("create database if not exists {0}", sourceDatabase);
    sql("create table {0}.{1} (" +
        " id int , data string , pt string " +
        ") partitioned by (pt) " +
        "stored as parquet ", sourceDatabase, sourceTable);

    sql("insert overwrite {0}.{1} " +
        " partition( pt = ''0001'' ) values \n" +
        " ( 1, ''aaa'' ), (2, ''bbb'' ) ", sourceDatabase, sourceTable);

    sql("insert overwrite {0}.{1} " +
        " partition( pt = ''0002'' ) values \n" +
        " ( 3, ''ccc'' ), (4, ''ddd'' ) ", sourceDatabase, sourceTable);

    sql("insert overwrite {0}.{1} " +
        " partition( pt = ''0003'' ) values \n" +
        " ( 5, ''eee'' ), (6, ''fff'' ) ", sourceDatabase, sourceTable);

    sql("migrate {0}.{1} to arctic {2}.{3}.{4} ",
        sourceDatabase, sourceTable,
        catalogName, database, table);

    rows = sql("select * from {0}.{1}.{2}", catalogName, database, table);
    Assert.assertEquals(6, rows.size());

    ArcticTable t = loadTable(catalogName, database, table);
    UnkeyedTable unkey = (UnkeyedTable) t ;
    StructLikeMap<List<DataFile>> partitionFiles = partitionFiles(unkey);
    Assert.assertEquals(3, partitionFiles.size());
  }

  @Test
  public void testMigrateNoPartitionTable(){
    sql("use spark_catalog");
    sql("create database if not exists {0}", sourceDatabase);
    sql("create table {0}.{1} (" +
        " id int , data string , pt string " +
        ") " +
        "stored as parquet ", sourceDatabase, sourceTable);

    sql("insert overwrite {0}.{1} values " +
        " ( 1, ''aaa'', ''0001'' ), \n " +
        " ( 2, ''bbb'', ''0001'' ), \n " +
        " ( 3, ''bbb'', ''0002'' ), \n" +
        " ( 4, ''bbb'', ''0002'' ), \n" +
        " ( 5, ''bbb'', ''0003'' ) ", sourceDatabase, sourceTable);


    sql("migrate {0}.{1} to arctic {2}.{3}.{4} ",
        sourceDatabase, sourceTable,
        catalogName, database, table);

    rows = sql("select * from {0}.{1}.{2}", catalogName, database, table);
    Assert.assertEquals(5, rows.size());

    ArcticTable t = loadTable(catalogName, database, table);
    UnkeyedTable unkey = (UnkeyedTable) t ;
    StructLikeMap<List<DataFile>> partitionFiles = partitionFiles(unkey);
    Assert.assertEquals(1, partitionFiles.size());
  }
}
