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

package io.crate.expression.reference;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.get.GetResult;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public final class Doc {

    private final GetResult getResult;
    private final Map<String, Object> source;
    private final Supplier<BytesRef> raw;

    public Doc(GetResult getResult) {
        this.getResult = getResult;
        this.source = getResult.getSource();
        this.raw = () -> getResult.sourceRef().toBytesRef();
    }

    public Doc(GetResult getResult, Map<String, Object> updatedSource) {
        this.getResult = getResult;
        this.source = updatedSource;
        this.raw = () -> {
            try {
                return XContentFactory.jsonBuilder().map(updatedSource).bytes().toBytesRef();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public long getVersion() {
        return getResult.getVersion();
    }

    public String getId() {
        return getResult.getId();
    }

    public BytesRef getRaw() {
        return raw.get();
    }

    public Map<String, Object> getSource() {
        return source;
    }

    public String getIndex() {
        return getResult.getIndex();
    }

    public Doc withUpdatedSource(HashMap<String, Object> updatedSource) {
        return new Doc(getResult, updatedSource);
    }

    public boolean isExists() {
        return getResult.isExists();
    }
}
