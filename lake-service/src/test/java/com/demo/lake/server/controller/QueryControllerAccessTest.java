package com.demo.lake.server.controller;

import com.demo.lake.common.governance.MaskingCatalog;
import com.demo.lake.server.security.ApiPrincipal;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 采样档位只由角色决定；缺省必须是最严档，不能因为没带身份就放行明文。 */
class QueryControllerAccessTest {

    @Test
    void 角色映射到脱敏档位() {
        assertEquals(MaskingCatalog.Access.AUDITOR, QueryController.accessOf(
                new ApiPrincipal("auditor", List.of("AUDITOR"))));
        assertEquals(MaskingCatalog.Access.ANALYST, QueryController.accessOf(
                new ApiPrincipal("analyst", List.of("QUERY", "ANALYST"))));
        assertEquals(MaskingCatalog.Access.RESTRICTED, QueryController.accessOf(
                new ApiPrincipal("viewer", List.of("QUERY"))));
        assertEquals(MaskingCatalog.Access.RESTRICTED, QueryController.accessOf(null));
    }
}
