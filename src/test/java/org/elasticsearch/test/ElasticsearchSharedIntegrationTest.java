/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test;

import org.apache.lucene.util.AbstractRandomizedTest;
import org.junit.*;

import java.io.IOException;

@Ignore
@AbstractRandomizedTest.IntegrationTests
public abstract class ElasticsearchSharedIntegrationTest extends ElasticsearchIntegrationTestBase {
    private static ElasticsearchSharedIntegrationTest INSTANCE = null;

    @AfterClass
    public static void shutDown() throws IOException {
        if (INSTANCE != null) {
            try {
                INSTANCE.afterInternal();
            } finally {
                INSTANCE = null;
            }
        }

    }

    @BeforeClass
    public final static void before() throws Exception {
        assert INSTANCE == null;
        // note we need to do this this way to make sure this is reproducible
        INSTANCE = (ElasticsearchSharedIntegrationTest) getContext().getTargetClass().newInstance();
        boolean success = false;
        try {
            INSTANCE.beforeInternal();
            INSTANCE.beforeTestStarts();
            success = true;
        } finally {
           if (!success) {
               shutDown();
           }
        }
    }

    protected abstract void beforeTestStarts() throws Exception;


}
