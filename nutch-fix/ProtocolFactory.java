/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.protocol;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.util.ObjectCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Creates and caches {@link Protocol} plugins. Protocol plugins should define
 * the attribute "protocolName" with the name of the protocol that they
 * implement. Configuration object is used for caching. Cache key is constructed
 * from appending protocol name (eg. http) to constant
 * {@link Protocol#X_POINT_ID}.
 */
public class ProtocolFactory {

    public static final Logger LOG = LoggerFactory
            .getLogger(ProtocolFactory.class);

    private ExtensionPoint extensionPoint;

    private Configuration conf;

    public ProtocolFactory(Configuration conf) {
        this.conf = conf;
        this.extensionPoint = PluginRepository.get(conf).getExtensionPoint(
                Protocol.X_POINT_ID);
        if (this.extensionPoint == null) {
            throw new RuntimeException("x-point " + Protocol.X_POINT_ID
                    + " not found.");
        }
    }

    /**
     * Returns the appropriate {@link Protocol} implementation for a url.
     *
     * @param urlString Url String
     * @return The appropriate {@link Protocol} implementation for a given
     * {@link URL}.
     * @throws ProtocolNotFound when Protocol can not be found for urlString
     */
    public Protocol getProtocol(String urlString)
            throws ProtocolNotFound {
        ObjectCache objectCache = ObjectCache.get(conf);
        try {
            URL url = new URL(urlString);
            String protocolName = url.getProtocol();
            if (protocolName == null)
                throw new ProtocolNotFound(urlString);

            String cacheId = Protocol.X_POINT_ID + protocolName;
            Protocol protocol = (Protocol) objectCache.getObject(cacheId);
            if (protocol != null) {
                return protocol;
            }
            synchronized (objectCache) {
                protocol = (Protocol) objectCache.getObject(cacheId);
                if (protocol == null) {
                    Extension extension = findExtension(protocolName);
                    if (extension == null) {
                        throw new ProtocolNotFound(protocolName);
                    }

                    protocol = (Protocol) extension.getExtensionInstance();
                    objectCache.setObject(cacheId, protocol);
                }
            }
            return protocol;
        } catch (MalformedURLException | PluginRuntimeException e) {
            throw new ProtocolNotFound(urlString, e.toString());
        }
    }

    private Extension findExtension(String name) throws PluginRuntimeException {

        Extension[] extensions = this.extensionPoint.getExtensions();

        for (int i = 0; i < extensions.length; i++) {
            Extension extension = extensions[i];

            if (contains(name, extension.getAttribute("protocolName")))
                return extension;
        }
        return null;
    }

    boolean contains(String what, String where) {
        String parts[] = where.split("[, ]");
        for (int i = 0; i < parts.length; i++) {
            if (parts[i].equals(what))
                return true;
        }
        return false;
    }

}
