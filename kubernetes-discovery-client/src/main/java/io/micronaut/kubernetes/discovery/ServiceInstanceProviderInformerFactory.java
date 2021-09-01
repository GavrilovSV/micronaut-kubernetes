/*
 * Copyright 2017-2021 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.kubernetes.discovery;

import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.micronaut.kubernetes.KubernetesConfiguration;
import io.micronaut.kubernetes.informer.SharedIndexInformerFactory;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Bean registers {@link io.kubernetes.client.informer.SharedIndexInformer} for the {@link KubernetesServiceInstanceProvider}.
 *
 * @author Pavol Gressa
 * @since 3.1
 */
@Singleton
public class ServiceInstanceProviderInformerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceInstanceProviderInformerFactory.class);

    private final KubernetesConfiguration kubernetesConfiguration;
    private final SharedIndexInformerFactory sharedIndexInformerFactory;
    private final KubernetesConfiguration.KubernetesDiscoveryConfiguration discoveryConfiguration;

    public ServiceInstanceProviderInformerFactory(SharedIndexInformerFactory sharedIndexInformerFactory,
                                                  KubernetesConfiguration kubernetesConfiguration,
                                                  KubernetesConfiguration.KubernetesDiscoveryConfiguration discoveryConfiguration) {
        this.kubernetesConfiguration = kubernetesConfiguration;
        this.sharedIndexInformerFactory = sharedIndexInformerFactory;
        this.discoveryConfiguration = discoveryConfiguration;
    }

    /**
     * Based on the provided {@link KubernetesServiceInstanceProvider} and {@link KubernetesServiceConfiguration} it
     * creates {@link SharedIndexInformer} for every namespace for which the given instance provider's mode is configured
     * to. Then the {@link InformerResourceCache} is configured for every created informer.
     *
     * @param instanceProvider instance provider
     * @param <T> Kubernetes object
     * @return resoruce cache
     */
    public <T extends KubernetesObject> InformerResourceCache<T> createInformersFor(KubernetesServiceInstanceProvider instanceProvider) {
        Set<String> namespaces = discoveryConfiguration.computeNamespacesForMode(instanceProvider.getMode());

        if (discoveryConfiguration.getMode().equalsIgnoreCase(instanceProvider.getMode())) {
            namespaces.add(kubernetesConfiguration.getNamespace());
        }

        InformerResourceCache namespacedInformerResourceCacheEventHandler = new InformerResourceCache();
        List<SharedIndexInformer<? extends KubernetesObject>> informers = sharedIndexInformerFactory.sharedIndexInformersFor(
                instanceProvider.getApiType(), instanceProvider.getApiListType(),
                instanceProvider.getResorucePlural(), new ArrayList<>(namespaces), null, null);
        informers.forEach(i -> i.addEventHandler(namespacedInformerResourceCacheEventHandler));

        if (LOG.isInfoEnabled()) {
            LOG.info("Informer<{}> for namespaces {} created for mode: {}", instanceProvider.getApiType(), namespaces, instanceProvider.getMode());
        }

        return namespacedInformerResourceCacheEventHandler;
    }
}
