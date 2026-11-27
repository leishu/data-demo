package com.demo.lake.server;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaParameter;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Set;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * 类名与 HTTP DTO 的命名守卫（规则表见 docs/NAMING.md）。
 *
 * <p>Checkstyle 在源码层管大小写、缩写黑名单和局部变量；这里在字节码层管「角色 → 后缀」，
 * 防止有人给 {@code @Service} 类起名 *Helper、给 {@code @RestController} 起名 *Endpoint，
 * 或者给 {@code @RequestBody} 换个 *Dto / *Vo / *Payload 的新叫法。</p>
 */
@AnalyzeClasses(packages = "com.demo.lake.server")
class NamingArchitectureTest {

    /** 控制器：@RestController → *Controller */
    @ArchTest
    static final ArchRule controllersEndWithController = classes()
            .that().areAnnotatedWith(RestController.class)
            .should().haveSimpleNameEndingWith("Controller");

    /** 用例编排：@Service → *Service（曾经有过叫 QuotaHelper 的 @Service，已收编） */
    @ArchTest
    static final ArchRule servicesEndWithService = classes()
            .that().areAnnotatedWith(Service.class)
            .should().haveSimpleNameEndingWith("Service");

    /** 含糊后缀一律禁用：Helper / Manager / Util / Impl */
    @ArchTest
    static final ArchRule noVagueSuffixes = noClasses()
            .should().haveSimpleNameEndingWith("Helper")
            .orShould().haveSimpleNameEndingWith("Manager")
            .orShould().haveSimpleNameEndingWith("Util")
            .orShould().haveSimpleNameEndingWith("Impl");

    /** 控制器里定义的 DTO 必须有角色后缀：请求体 *Request / 嵌套条目 *Item / 响应 *View */
    @ArchTest
    static final ArchRule controllerDtosHaveRoleSuffix = classes()
            .that(declaredInRestController())
            .should().haveSimpleNameEndingWith("Request")
            .orShould().haveSimpleNameEndingWith("Item")
            .orShould().haveSimpleNameEndingWith("View");

    /** 领域包（batch/governance/query/observability）下只允许 model / service / store 三层 */
    @ArchTest
    static final ArchRule domainPackagesUseModelServiceStore = classes()
            .that(inDomainPackage())
            .should(new ArchCondition<JavaClass>("只放在 model / service / store 子包") {
                @Override
                public void check(JavaClass clazz, ConditionEvents events) {
                    String packageName = clazz.getPackageName();
                    String layer = packageName.substring(packageName.lastIndexOf('.') + 1);
                    if (!Set.of("model", "service", "store").contains(layer)) {
                        events.add(SimpleConditionEvent.violated(clazz,
                                clazz.getFullName() + " 在 " + packageName + "，领域包只允许 model/service/store"));
                    }
                }
            });

    /** @RequestBody 的参数类型统一叫 *Request，参数名统一叫 request */
    @ArchTest
    static final ArchRule requestBodiesEndWithRequest = methods()
            .that(haveRequestBodyParameter())
            .should(new ArchCondition<JavaMethod>("@RequestBody 参数类型以 Request 结尾") {
                @Override
                public void check(JavaMethod method, ConditionEvents events) {
                    for (JavaParameter parameter : method.getParameters()) {
                        if (!parameter.isAnnotatedWith(RequestBody.class)) {
                            continue;
                        }
                        String type = parameter.getRawType().getSimpleName();
                        if (!type.endsWith("Request")) {
                            events.add(SimpleConditionEvent.violated(method,
                                    method.getFullName() + " 的 @RequestBody 类型 " + type + " 不以 Request 结尾"));
                        }
                    }
                }
            });

    private static DescribedPredicate<JavaClass> inDomainPackage() {
        return new DescribedPredicate<>("batch/governance/query/observability 领域包下的类") {
            @Override
            public boolean test(JavaClass clazz) {
                String prefix = "com.demo.lake.server.";
                String packageName = clazz.getPackageName();
                if (!packageName.startsWith(prefix)) {
                    return false;
                }
                String rest = packageName.substring(prefix.length());
                int dot = rest.indexOf('.');
                String domain = dot < 0 ? rest : rest.substring(0, dot);
                return Set.of("batch", "governance", "query", "observability").contains(domain);
            }
        };
    }

    private static DescribedPredicate<JavaClass> declaredInRestController() {
        return new DescribedPredicate<>("定义在 @RestController 内的 DTO") {
            @Override
            public boolean test(JavaClass clazz) {
                return clazz.isNestedClass()
                        && clazz.getEnclosingClass()
                                .filter(enclosing -> enclosing.isAnnotatedWith(RestController.class))
                                .isPresent();
            }
        };
    }

    private static DescribedPredicate<JavaMethod> haveRequestBodyParameter() {
        return new DescribedPredicate<>("带 @RequestBody 参数的方法") {
            @Override
            public boolean test(JavaMethod method) {
                return method.getParameters().stream().anyMatch(p -> p.isAnnotatedWith(RequestBody.class));
            }
        };
    }
}
