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
import io.kubernetes.client.informer.ResourceEventHandler;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The {@link ResourceEventHandler} that caches the namespaced resources and is used for multiple {@link io.kubernetes.client.informer.SharedIndexInformer}.
 *
 * @param <ApiType> api type of the cache
 * @author Pavol Gressa
 * @since 3.1
 */
public class InformerResourceCache<ApiType extends KubernetesObject> implements ResourceEventHandler<ApiType> {

    private static final Logger LOG = LoggerFactory.getLogger(InformerResourceCache.class);

    private final List<ApiType> resourceList;

    public InformerResourceCache() {
        this.resourceList = Collections.synchronizedList(new ArrayList<>());
    }

    @Override
    public void onAdd(ApiType obj) {
        if (LOG.isDebugEnabled()) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Added<{}>: {}", obj.getClass(), obj);
            } else {
                LOG.debug("Added<{}>: {}/{}", obj.getClass(), obj.getMetadata().getNamespace(), obj.getMetadata().getName());
            }
        }
        resourceList.add(obj);
    }

    @Override
    public void onUpdate(ApiType oldObj, ApiType newObj) {
        if (LOG.isDebugEnabled()) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Updated<{}>: {}", newObj.getClass(), newObj);
            } else {
                LOG.debug("Updated<{}>: {}/{}", newObj.getClass(), newObj.getMetadata().getNamespace(), newObj.getMetadata().getName());
            }
        }
        resourceList.remove(oldObj);
        resourceList.add(newObj);
    }

    @Override
    public void onDelete(ApiType obj, boolean deletedFinalStateUnknown) {
        if (LOG.isDebugEnabled()) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Deleted<{}>: {}", obj.getClass(), obj);
            } else {
                LOG.debug("Deleted<{}>: {}/{}", obj.getClass(), obj.getMetadata().getNamespace(), obj.getMetadata().getName());
            }
        }
        resourceList.remove(obj);
    }

    /**
     * Get resource from the cache.
     *
     * @param name      resource name
     * @param namespace resource namesapce
     * @return mono with the resource or empty mono
     */
    public Mono<ApiType> getResource(@NonNull String name, @Nullable String namespace) {
        Optional<ApiType> resources = resourceList.stream().filter(e -> {
            if (e.getMetadata() != null) {
                return Objects.equals(e.getMetadata().getName(), name) &&
                        (namespace == null || Objects.equals(e.getMetadata().getNamespace(), namespace));
            }
            return true;
        }).findFirst();
        return resources.map(Mono::just).orElseGet(Mono::empty);
    }

    /**
     * Get all resources from the cache for given {@code namespace}.
     *
     * @param namespace namespace
     * @return mono with resources or empty mono
     */
    public Flux<ApiType> getResources(@Nullable String namespace) {
        Flux<ApiType> flux = Flux.fromIterable(resourceList);
        if (namespace != null) {
            flux = flux.filter(r -> {
                if (r.getMetadata() != null) {
                    return Objects.equals(r.getMetadata().getNamespace(), namespace);
                }
                return true;
            });
        }
        return flux;
    }
}
