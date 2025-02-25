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

package com.netease.arctic.ams.server.config;

/**
 * @author hengshu
 * @version 1.0
 * Create 2021/11/17
 * Update
 */
public class ArcticMetaStoreConf {
  public static final ConfigOption<Long> SERVER_MAX_MESSAGE_SIZE =
      ConfigOptions.key("arctic.ams.server.max.message.size")
          .longType()
          .defaultValue(100 * 1024 * 1024L)
          .withDescription("Maximum message size in bytes a AMS will accept.");
  public static final ConfigOption<Integer> SERVER_MAX_THREADS =
      ConfigOptions.key("arctic.ams.server.max.threads")
          .intType()
          .defaultValue(1000)
          .withDescription("Maximum number of worker threads in the Thrift server's pool.");
  public static final ConfigOption<Integer> SERVER_MIN_THREADS =
      ConfigOptions.key("arctic.ams.server.min.threads")
          .intType()
          .defaultValue(200)
          .withDescription("Minimum number of worker threads in the Thrift server's pool.");
  public static final ConfigOption<Integer> THRIFT_BIND_PORT =
      ConfigOptions.key("arctic.ams.thrift.port")
          .intType()
          .defaultValue(9090)
          .withDescription("Arctic ams listener port");
  public static final ConfigOption<Integer> HTTP_SERVER_PORT =
      ConfigOptions.key("arctic.ams.http.port")
          .intType()
          .defaultValue(19090)
          .withDescription("Arctic ams http listener port");

  public static final ConfigOption<Boolean> TCP_KEEP_ALIVE =
      ConfigOptions.key("arctic.ams.server.tcp.keepalive")
          .booleanType()
          .defaultValue(true)
          .withDescription("Whether to enable TCP keepalive for the ams server. " +
              "Keepalive will prevent accumulation of half-open connections.");
  public static final ConfigOption<Long> OPTIMIZE_CHECK_STATUS_INTERVAL =
      ConfigOptions.key("arctic.ams.optimize.check-status.interval")
          .longType()
          .defaultValue(60000L)
          .withDescription("Optimize status check interval.");
  public static final ConfigOption<Integer> OPTIMIZE_CHECK_THREAD_POOL_SIZE =
      ConfigOptions.key("arctic.ams.optimize.check.thread.pool-size")
          .intType()
          .defaultValue(3)
          .withDescription("Number of threads in the thread pool.  " +
              "These will be used to execute all optimize check processes.");
  public static final ConfigOption<Integer> OPTIMIZE_COMMIT_THREAD_POOL_SIZE =
      ConfigOptions.key("arctic.ams.optimize.commit.thread.pool-size")
          .intType()
          .defaultValue(10)
          .withDescription("Number of threads in the thread pool.  " +
              "These will be used to execute all optimize commit processes.");
  public static final ConfigOption<Integer> EXPIRE_THREAD_POOL_SIZE =
      ConfigOptions.key("arctic.ams.expire.thread.pool-size")
          .intType()
          .defaultValue(10)
          .withDescription("Number of threads in the thread pool.  " +
              "These will be used to execute all expire processes.");
  public static final ConfigOption<Integer> ORPHAN_CLEAN_THREAD_POOL_SIZE =
      ConfigOptions.key("arctic.ams.orphan.clean.thread.pool-size")
          .intType()
          .defaultValue(10)
          .withDescription("Number of threads in the thread pool.  " +
              "These will be used to execute all orphan file clean processes.");
  public static final ConfigOption<Integer> SYNC_FILE_INFO_CACHE_THREAD_POOL_SIZE =
      ConfigOptions.key("arctic.ams.file.sync.thread.pool-size")
          .intType()
          .defaultValue(10)
          .withDescription("Number of threads in the thread pool.  " +
              "These will be used to execute all file sync processes.");
  public static final ConfigOption<String> THRIFT_BIND_HOST =
      ConfigOptions.key("arctic.ams.server-host")
          .stringType()
          .defaultValue("")
          .withDescription("Bind host on which to run the ams thrift service.");
  public static final ConfigOption<Boolean> USE_THRIFT_COMPACT_PROTOCOL =
      ConfigOptions.key("arctic.ams.thrift.compact.protocol.enabled")
          .booleanType()
          .defaultValue(false)
          .withDescription("If true, the ams Thrift interface will use TCompactProtocol. " +
              "When false (default) TBinaryProtocol will be used.\n" +
              "Setting it to true will break compatibility with older clients running TBinaryProtocol.");
  public static final ConfigOption<String> MYBATIS_CONNECTION_URL =
      ConfigOptions.key("arctic.ams.mybatis.ConnectionURL")
          .stringType()
          .defaultValue("jdbc:mysql://127.0.0.1:3306/metadata?" +
              "serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF8" +
              "&autoReconnect=true&useAffectedRows=true")
          .withDescription("The connection URL which to restore metadata.");
  public static final ConfigOption<String> MYBATIS_CONNECTION_DRIVER_CLASS_NAME =
      ConfigOptions.key("arctic.ams.mybatis.ConnectionDriverClassName")
          .stringType()
          .defaultValue("com.mysql.jdbc.Driver")
          .withDescription("The connection driver class name which to connect database.");
  public static final ConfigOption<String> MYBATIS_CONNECTION_USER_NAME =
      ConfigOptions.key("arctic.ams.mybatis.ConnectionUserName")
          .stringType()
          .defaultValue("root")
          .withDescription("The user name in database.");
  public static final ConfigOption<String> MYBATIS_CONNECTION_PASSWORD =
      ConfigOptions.key("arctic.ams.mybatis.ConnectionPassword")
          .stringType()
          .defaultValue("93299")
          .withDescription("The password in database.");
  public static final ConfigOption<String> DB_TYPE =
      ConfigOptions.key("arctic.ams.database.type")
          .stringType()
          .defaultValue("mysql")
          .withDescription("Restore database type.");
  public static final ConfigOption<Boolean> ADAPT_HIVE_CLEAN_STALE_CHANGE_FILES_DEFAULT =
      ConfigOptions.key("adapt.hive.stale-change-files.clean.default")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether to clean stale change files after plan");
  public static final ConfigOption<Long> FILE_CACHE_EXPIRED_INTERVAL =
      ConfigOptions.key("file.cache.expired.interval")
          .longType()
          .defaultValue(15 * 24 * 60 * 60 * 1000L)
          .withDescription("file cache expired interval");
  public static final ConfigOption<Long> TABLE_FILE_INFO_CACHE_INTERVAL =
      ConfigOptions.key("table.file.info.cache.interval")
          .longType()
          .defaultValue(6 * 60 * 60 * 1000L)
          .withDescription("table file info will be sync-cache when there is long time no cache data.");
  public static final ConfigOption<String> SYSTEM_EXTENSION_PROPERTIES =
      ConfigOptions.key("system.extension.properties")
          .stringType()
          .defaultValue("")
          .withDescription("system extension properties.");
  public static final ConfigOption<String> ARCTIC_HOME =
      ConfigOptions.key("ARCTIC_HOME")
          .stringType()
          .defaultValue("")
          .withDescription("arctic install path.");
}
