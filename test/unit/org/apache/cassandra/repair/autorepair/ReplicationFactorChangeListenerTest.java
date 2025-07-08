/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.repair.autorepair;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReplicationFactorChangeListenerTest
{
    @Test
    public void testNTSRFIncrease()
    {
        ReplicationFactorChangeListener listener = new ReplicationFactorChangeListener();
        
        Map<String, String> beforeOptions = new HashMap<>();
        beforeOptions.put("dc1", "2");
        beforeOptions.put("dc2", "1");
        
        Map<String, String> afterOptions = new HashMap<>();
        afterOptions.put("dc1", "3");  // RF increased
        afterOptions.put("dc2", "1");
        
        KeyspaceMetadata before = createKeyspace("test_ks", beforeOptions);
        KeyspaceMetadata after = createKeyspace("test_ks", afterOptions);
        
        // This would normally trigger repair scheduling
        listener.onAlterKeyspace(before, after);
    }
    
    @Test
    public void testNTSNewDCAdded()
    {
        ReplicationFactorChangeListener listener = new ReplicationFactorChangeListener();
        
        Map<String, String> beforeOptions = new HashMap<>();
        beforeOptions.put("dc1", "2");
        
        Map<String, String> afterOptions = new HashMap<>();
        afterOptions.put("dc1", "2");
        afterOptions.put("dc2", "1");  // New DC added
        
        KeyspaceMetadata before = createKeyspace("test_ks", beforeOptions);
        KeyspaceMetadata after = createKeyspace("test_ks", afterOptions);
        
        listener.onAlterKeyspace(before, after);
    }
    
    private KeyspaceMetadata createKeyspace(String name, Map<String, String> replicationOptions)
    {
        ReplicationParams replication = ReplicationParams.nts(replicationOptions);
        KeyspaceParams params = KeyspaceParams.create(false, replication);
        return KeyspaceMetadata.create(name, params);
    }
}