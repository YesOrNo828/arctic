/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.netease.arctic.trino;

import com.netease.arctic.table.TableMetaStore;
import io.trino.plugin.hive.authentication.GenericExceptionAction;
import io.trino.plugin.hive.authentication.HdfsAuthentication;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.security.ConnectorIdentity;
import javax.inject.Inject;

public class ArcticHdfsAuthentication implements HdfsAuthentication {

  private ArcticCatalogFactory arcticCatalogFactory;

  private TableMetaStore tableMetaStore;

  @Inject
  public ArcticHdfsAuthentication(ArcticCatalogFactory arcticCatalogFactory) {
    this.arcticCatalogFactory = arcticCatalogFactory;
    this.tableMetaStore =
        ((ArcticCatalogSupportTableSuffix) arcticCatalogFactory.getArcticCatalog()).getTableMetaStore();
  }

  @Override
  public <R, E extends Exception> R doAs(ConnectorIdentity identity, GenericExceptionAction<R, E> action) throws E {
    try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(this.getClass().getClassLoader())) {
      return tableMetaStore.doAs(() -> action.run());
    }
  }
}
