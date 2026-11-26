/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.servicecomb.core.filter;

/**
 * Thrown when a filter related configuration item can not be parsed,
 * e.g. {@code servicecomb.filter.<name>.<application>.<service>.order=abc}.
 *
 * <p>The message always carries the configuration key, the filter name and the
 * target microservice, so that logs are actionable.</p>
 *
 * <p>{@link InvocationFilterChains} treats this exception specially: when a chain
 * rebuild caused by a configuration change fails with this exception, the last
 * successfully resolved chain is kept, instead of leaving the service with a
 * broken chain.</p>
 */
public class FilterConfigException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private final String configKey;

  private final String application;

  private final String serviceName;

  public FilterConfigException(String configKey, String application, String serviceName, String message,
      Throwable cause) {
    super(String.format("invalid filter configuration, key=%s, application=%s, service=%s: %s",
        configKey, application, serviceName, message), cause);
    this.configKey = configKey;
    this.application = application;
    this.serviceName = serviceName;
  }

  public String getConfigKey() {
    return configKey;
  }

  public String getApplication() {
    return application;
  }

  public String getServiceName() {
    return serviceName;
  }
}
