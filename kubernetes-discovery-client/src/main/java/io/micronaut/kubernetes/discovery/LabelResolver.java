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

import io.reactivex.Flowable;

import java.util.List;
import java.util.Map;

/**
 * Label resolver is utility class that provides the label fetching methods.
 *
 * @author Pavol Gressa
 * @since 2.4
 */
public interface LabelResolver {

    /**
     * Resolves labels from the POD this application runs in.
     *
     * @param podLabelKeys label keys to resolve
     * @return resolved key,value map of keys
     */
    Flowable<Map<String, String>> resolveCurrentPodLabels(List<String> podLabelKeys);


    /**
     * Resolves labels of the specified pod.
     *
     * @param podName      name of the pod
     * @param podLabelKeys label keys to resolve
     * @return resolved key,value map of keys
     */
    Flowable<Map<String, String>> resolvePodLabels(String podName, List<String> podLabelKeys);
}
