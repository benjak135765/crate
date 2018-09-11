/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SearchPathTest {

    @Test
    public void testEmptyConstructorSetsDefaultSchema() {
        List<String> searchPath = new SearchPath().searchPath();
        assertThat(searchPath.size(), is(2));
        assertThat(searchPath.get(1), is(Schemas.DOC_SCHEMA_NAME));
    }

    @Test
    public void testCurrentSchemaIsFirstSchemaInSearchPath() {
        SearchPath searchPath = new SearchPath(ImmutableList.of("firstSchema", "secondSchema"));
        assertThat(searchPath.currentSchema(), is("firstSchema"));
    }

    @Test
    public void testDefaultSchemaIsFirstSchemaInSearchPath() {
        SearchPath searchPath = new SearchPath(ImmutableList.of("firstSchema", "secondSchema"));
        assertThat(searchPath.defaultSchema(), is("firstSchema"));
    }

    @Test
    public void testPgCatalogIsFirstInTheSearchPathIfNotExplicitlySet() {
        SearchPath searchPath = new SearchPath(ImmutableList.of("firstSchema", "secondSchema"));
        assertThat(searchPath.searchPath().get(0), is(SearchPath.PG_CATALOG_SCHEMA));
    }

    @Test
    public void testPgCatalogKeepsPositionInSearchPathWhenExplicitlySet() {
        SearchPath searchPath = new SearchPath(ImmutableList.of("firstSchema", "secondSchema", SearchPath.PG_CATALOG_SCHEMA));
        assertThat(searchPath.searchPath().get(2), is(SearchPath.PG_CATALOG_SCHEMA));
    }

    @Test
    public void testPgCatalogIsDefaultAndCurrentSchemaIfSetFirstInPath() {
        SearchPath searchPath = new SearchPath(ImmutableList.of(SearchPath.PG_CATALOG_SCHEMA, "secondSchema"));
        assertThat(searchPath.defaultSchema(), is(SearchPath.PG_CATALOG_SCHEMA));
        assertThat(searchPath.currentSchema(), is(SearchPath.PG_CATALOG_SCHEMA));
    }
}
