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

package com.netease.arctic.ams.server.service;

import com.google.common.annotations.VisibleForTesting;
import com.netease.arctic.ams.server.handler.impl.ArcticTableMetastoreHandler;
import com.netease.arctic.ams.server.handler.impl.OptimizeManagerHandler;
import com.netease.arctic.ams.server.optimize.IOptimizeService;
import com.netease.arctic.ams.server.optimize.OptimizeService;
import com.netease.arctic.ams.server.service.impl.ArcticTransactionService;
import com.netease.arctic.ams.server.service.impl.CatalogMetadataService;
import com.netease.arctic.ams.server.service.impl.ContainerMetaService;
import com.netease.arctic.ams.server.service.impl.FileInfoCacheService;
import com.netease.arctic.ams.server.service.impl.JDBCMetaService;
import com.netease.arctic.ams.server.service.impl.OptimizeExecuteService;
import com.netease.arctic.ams.server.service.impl.OptimizeQueueService;
import com.netease.arctic.ams.server.service.impl.OptimizerService;
import com.netease.arctic.ams.server.service.impl.OrphanFilesCleanService;
import com.netease.arctic.ams.server.service.impl.QuotaService;
import com.netease.arctic.ams.server.service.impl.TableBaseInfoService;
import com.netease.arctic.ams.server.service.impl.TableExpireService;
import com.netease.arctic.ams.server.service.impl.TableTaskHistoryService;

public class ServiceContainer {
  private static volatile IOptimizeService optimizeService;
  
  private static volatile ITableExpireService tableExpireService;
  
  private static volatile IOrphanFilesCleanService orphanFilesCleanService;

  private static volatile OptimizeQueueService optimizeQueueService;

  private static volatile IMetaService metaService;
  
  private static volatile IQuotaService quotaService;
  private static volatile OptimizeExecuteService optimizeExecuteService;

  private static volatile OptimizeManagerHandler optimizeManagerHandler;

  private static volatile OptimizerService optimizerService;

  private static volatile ContainerMetaService containerMetaService;

  private static volatile CatalogMetadataService catalogMetadataService;

  private static volatile FileInfoCacheService fileInfoCacheService;

  private static volatile ITableTaskHistoryService tableTaskHistoryService;

  private static volatile ArcticTransactionService arcticTransactionService;

  private static volatile ITableInfoService tableInfoService;

  private static volatile ArcticTableMetastoreHandler tableMetastoreHandler;

  public static IOptimizeService getOptimizeService() {
    if (optimizeService == null) {
      synchronized (ServiceContainer.class) {
        optimizeService = new OptimizeService();
      }
    }

    return optimizeService;
  }

  public static ITableExpireService getTableExpireService() {
    if (tableExpireService == null) {
      synchronized (ServiceContainer.class) {
        tableExpireService = new TableExpireService();
      }
    }

    return tableExpireService;
  }
  
  public static IOrphanFilesCleanService getOrphanFilesCleanService() {
    if (orphanFilesCleanService == null) {
      synchronized (ServiceContainer.class) {
        orphanFilesCleanService = new OrphanFilesCleanService();
      }
    }

    return orphanFilesCleanService;
  }

  public static OptimizerService getOptimizerService() {
    if (optimizerService == null) {
      synchronized (ServiceContainer.class) {
        if (optimizerService == null) {
          optimizerService = new OptimizerService();
        }
      }
    }
    return optimizerService;
  }

  public static OptimizeManagerHandler getOptimizeManagerHandler() {
    if (optimizeManagerHandler == null) {
      synchronized (ServiceContainer.class) {
        if (optimizeManagerHandler == null) {
          optimizeManagerHandler = new OptimizeManagerHandler();
        }
      }
    }
    return optimizeManagerHandler;
  }


  public static OptimizeQueueService getOptimizeQueueService() {
    if (optimizeQueueService == null) {
      synchronized (ServiceContainer.class) {
        optimizeQueueService = new OptimizeQueueService();
      }
    }

    return optimizeQueueService;
  }

  public static IMetaService getMetaService() {
    if (metaService == null) {
      synchronized (ServiceContainer.class) {
        metaService = new JDBCMetaService();
      }
    }

    return metaService;
  }

  public static IQuotaService getQuotaService() {
    if (quotaService == null) {
      synchronized (ServiceContainer.class) {
        quotaService = new QuotaService(getTableTaskHistoryService(), getMetaService());
      }
    }

    return quotaService;
  }

  public static CatalogMetadataService getCatalogMetadataService() {
    if (catalogMetadataService == null) {
      synchronized (ServiceContainer.class) {
        if (catalogMetadataService == null) {
          catalogMetadataService = new CatalogMetadataService();
        }
      }
    }

    return catalogMetadataService;
  }

  public static FileInfoCacheService getFileInfoCacheService() {
    if (fileInfoCacheService == null) {
      synchronized (ServiceContainer.class) {
        if (fileInfoCacheService == null) {
          fileInfoCacheService = new FileInfoCacheService();
        }
      }
    }

    return fileInfoCacheService;
  }

  public static ArcticTransactionService getArcticTransactionService() {
    if (arcticTransactionService == null) {
      synchronized (ServiceContainer.class) {
        if (arcticTransactionService == null) {
          arcticTransactionService = new ArcticTransactionService();
        }
      }
    }

    return arcticTransactionService;
  }

  public static ITableTaskHistoryService getTableTaskHistoryService() {
    if (tableTaskHistoryService == null) {
      synchronized (ServiceContainer.class) {
        tableTaskHistoryService = new TableTaskHistoryService();
      }
    }

    return tableTaskHistoryService;
  }

  public static ITableInfoService getTableInfoService() {
    if (tableInfoService == null) {
      synchronized (ServiceContainer.class) {
        tableInfoService = new TableBaseInfoService(getMetaService());
      }
    }
    return tableInfoService;
  }

  public static ArcticTableMetastoreHandler getTableMetastoreHandler() {
    if (tableMetastoreHandler == null) {
      synchronized (ServiceContainer.class) {
        tableMetastoreHandler = new ArcticTableMetastoreHandler(getMetaService());
      }
    }
    return tableMetastoreHandler;
  }

  @VisibleForTesting
  public static void setMetaService(IMetaService imetaService) {
    metaService = imetaService;
  }

  @VisibleForTesting
  public static void setFileInfoCacheService(FileInfoCacheService testFileInfoCacheService) {
    fileInfoCacheService = testFileInfoCacheService;
  }

  @VisibleForTesting
  public static void setOptimizeService(IOptimizeService optimizeService) {
    ServiceContainer.optimizeService = optimizeService;
  }

  @VisibleForTesting
  public static void setTableTaskHistoryService(ITableTaskHistoryService tableHistoryService) {
    tableTaskHistoryService = tableHistoryService;
  }

  public static OptimizeExecuteService getOptimizeExecuteService() {
    if (optimizeExecuteService == null) {
      synchronized (ServiceContainer.class) {
        if (optimizeExecuteService == null) {
          optimizeExecuteService = new OptimizeExecuteService();
        }
      }
    }
    return optimizeExecuteService;
  }

  public static ContainerMetaService getContainerMetaService() {
    if (containerMetaService == null) {
      synchronized (ServiceContainer.class) {
        if (containerMetaService == null) {
          containerMetaService = new ContainerMetaService();
        }
      }
    }
    return containerMetaService;
  }
}
