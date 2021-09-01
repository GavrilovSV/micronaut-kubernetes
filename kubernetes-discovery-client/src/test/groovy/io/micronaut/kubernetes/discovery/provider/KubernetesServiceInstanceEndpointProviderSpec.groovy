package io.micronaut.kubernetes.discovery.provider

import io.fabric8.kubernetes.api.model.*
import io.fabric8.kubernetes.client.dsl.MixedOperation
import io.fabric8.kubernetes.client.dsl.Resource
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
import io.micronaut.context.env.Environment
import io.micronaut.kubernetes.discovery.KubernetesServiceConfiguration
import io.micronaut.kubernetes.test.KubernetesSpecification
import io.micronaut.kubernetes.test.TestUtils
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import reactor.core.publisher.Flux
import spock.lang.Requires
import spock.lang.Unroll

@MicronautTest(environments = [Environment.KUBERNETES])
@Requires({ TestUtils.kubernetesApiAvailable() })
@Property(name = "kubernetes.client.namespace", value = "micronaut-endpoint-provider")
@Property(name = "spec.reuseNamespace", value = "false")
class KubernetesServiceInstanceEndpointProviderSpec extends KubernetesSpecification {

    Map<String, Object> getConfig(boolean watchEnabled, Map<String, Object> additional = [:]) {
        def config = [
                "kubernetes.client.namespace"                                          : namespace,
                "kubernetes.client.discovery.mode"                                     : "endpoint",
                "kubernetes.client.discovery.mode-configuration.endpoint.watch.enabled": watchEnabled
        ]
        config.putAll(additional as Map<? extends String, ? extends String>)
        return config
    }

    @Unroll
    void "it returns nothing when service endpoints doesn't exists [watchEnabled=#watchEnabled]"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                getConfig(watchEnabled),
                Environment.KUBERNETES)
        KubernetesServiceInstanceEndpointProvider provider = applicationContext.getBean(KubernetesServiceInstanceEndpointProvider)

        when:
        def config = createConfig("a-service")
        def instanceList = Flux.from(provider.getInstances(config)).blockFirst()

        then:
        instanceList.size() == 0

        cleanup:
        applicationContext.close()

        where:
        watchEnabled << [true, false]
    }

    @Unroll
    void "it can get headless service ip by using mode endpoint [watchEnabled=#watchEnabled]"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                getConfig(watchEnabled),
                Environment.KUBERNETES)
        KubernetesServiceInstanceEndpointProvider provider = applicationContext.getBean(KubernetesServiceInstanceEndpointProvider)

        Service service = operations.createService("example-headless-service", namespace,
                new ServiceSpecBuilder()
                        .withClusterIP("None")
                        .withPorts(
                                new ServicePortBuilder()
                                        .withPort(8081)
                                        .withTargetPort(new IntOrString(8081))
                                        .build()
                        )
                        .withSelector(["app": "example-service"])
                        .build())

        when:
        def config = createConfig("example-headless-service")
        def instanceList = Flux.from(provider.getInstances(config)).blockFirst()

        then:
        instanceList.size() == 2

        cleanup:
        operations.deleteService(service)
        applicationContext.close()

        where:
        watchEnabled << [true, false]
    }

    @Unroll
    void "it gets port ip for multi port service [watchEnabled=#watchEnabled]"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                getConfig(watchEnabled),
                Environment.KUBERNETES)
        KubernetesServiceInstanceEndpointProvider provider = applicationContext.getBean(KubernetesServiceInstanceEndpointProvider)

        Service service = operations.createService("multiport-service", namespace,
                new ServiceSpecBuilder()
                        .withPorts(
                                new ServicePortBuilder()
                                        .withName("jvm-debug")
                                        .withPort(5004)
                                        .withTargetPort(new IntOrString("jvm-debug"))
                                        .build(),
                                new ServicePortBuilder()
                                        .withName("http")
                                        .withPort(8081)
                                        .withTargetPort(new IntOrString("http"))
                                        .build()
                        )
                        .withSelector(["app": "example-service"])
                        .build())

        when: 'no port is specified'
        def config = createConfig("multiport-service")


        then: 'the returned list is empty'
        Flux.from(provider.getInstances(config)).blockFirst().isEmpty()

        when: 'http port is specified'
        config.port = 'http'
        def instances = Flux.from(provider.getInstances(config)).blockFirst()

        then: 'two service instances with port 8081 are discovered'
        instances.size() == 2
        instances.stream().allMatch(s -> s.port == 8081)

        cleanup:
        operations.deleteService(service)
        applicationContext.close()

        where:
        watchEnabled << [true, false]
    }

    @Unroll
    void "it can get service from other then app namespace [watchEnabled=#watchEnabled]"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                getConfig(watchEnabled, ["kubernetes.client.discovery.services.example-service": ["namespace": "other-namespace"]]),
                Environment.KUBERNETES)
        KubernetesServiceInstanceEndpointProvider provider = applicationContext.getBean(KubernetesServiceInstanceEndpointProvider)

        createNamespaceSafe("other-namespace")
        createBaseResources("other-namespace")
        createExampleServiceDeployment("other-namespace")

        def config = createConfig("example-service")
        config.namespace = "other-namespace"

        when:
        def instances = Flux.from(provider.getInstances(config)).blockFirst()

        then:
        instances.size() == 2
        operations.getEndpoints("example-service", "other-namespace").subsets.stream()
                .allMatch(e ->
                        e.addresses.stream().allMatch(address ->
                                instances.any { it.host == address.ip }
                        )
                )

        cleanup:
        operations.deleteNamespace("other-namespace")
        applicationContext.close()

        where:
        watchEnabled << [true, false]
    }

    @Unroll
    void "it ignores includes filter for manually configured service [watchEnabled=#watchEnabled]"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                getConfig(watchEnabled, ["kubernetes.client.discovery.includes": "example-service"]),
                Environment.KUBERNETES)
        KubernetesServiceInstanceEndpointProvider provider = applicationContext.getBean(KubernetesServiceInstanceEndpointProvider)
        sleep(500) // sleep for a second to let the informer startup

        when:
        def config = createConfig("example-client", true)
        def instanceList = Flux.from(provider.getInstances(config)).blockFirst()

        then:
        instanceList.size() == 1

        cleanup:
        applicationContext.close()

        where:
        watchEnabled << [true, false]
    }

    @Unroll
    void "it ignores excludes filter for manually configured service [watchEnabled=#watchEnabled]"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                getConfig(watchEnabled, ["kubernetes.client.discovery.excludes": "example-service"]),
                Environment.KUBERNETES)
        KubernetesServiceInstanceEndpointProvider provider = applicationContext.getBean(KubernetesServiceInstanceEndpointProvider)
        sleep(500) // sleep for a second to let the informer startup

        when:
        def config = createConfig("example-service", true)
        def instanceList = Flux.from(provider.getInstances(config)).blockFirst()

        then:
        instanceList.size() == 2

        cleanup:
        applicationContext.close()

        where:
        watchEnabled << [true, false]
    }

    @Unroll
    void "it ignores label filter for manually configured service [watchEnabled=#watchEnabled]"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                getConfig(watchEnabled, ["kubernetes.client.discovery.labels": [foo: "bar"]]),
                Environment.KUBERNETES)
        KubernetesServiceInstanceEndpointProvider provider = applicationContext.getBean(KubernetesServiceInstanceEndpointProvider)
        sleep(500) // sleep for a second to let the informer startup

        when:
        def config = createConfig("example-client", true)
        def instanceList = Flux.from(provider.getInstances(config)).blockFirst()

        then:
        instanceList.size() == 1

        when:
        config = createConfig("example-service", true)
        instanceList = Flux.from(provider.getInstances(config)).blockFirst()

        then:
        instanceList.size() == 2

        cleanup:
        applicationContext.close()

        where:
        watchEnabled << [true, false]
    }

    @Unroll
    void "it doesn't fail when service endpoint has no ip addresses [watchEnabled=#watchEnabled]"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                getConfig(watchEnabled),
                Environment.KUBERNETES)
        KubernetesServiceInstanceEndpointProvider provider = applicationContext.getBean(KubernetesServiceInstanceEndpointProvider)
        def endpointsOperations = operations.getClient(namespace).endpoints()
        Endpoints endpoints = endpointsOperations.create(new EndpointsBuilder()
                .withNewMetadata()
                .withName("empty-endpoint")
                .endMetadata()
                .build())
        sleep(500) // sleep for a second to let the informer startup

        when:
        def config = createConfig("empty-endpoint", true)
        def instanceList = Flux.from(provider.getInstances(config)).blockFirst()

        then:
        instanceList.size() == 0

        cleanup:
        endpointsOperations.delete(endpoints)
        applicationContext.close()

        where:
        watchEnabled << [true, false]
    }

    KubernetesServiceConfiguration createConfig(String name, manual = false) {
        return new KubernetesServiceConfiguration(
                name,
                name,
                namespace,
                KubernetesServiceInstanceEndpointProvider.MODE,
                null,
                manual)
    }
}
