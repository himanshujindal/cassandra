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

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.ReplicationFactor;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.service.AutoRepairService;

/**
 * Listens for keyspace replication factor changes and triggers appropriate repair actions.
 */
public class ReplicationFactorChangeListener implements SchemaChangeListener
{
    private static final Logger logger = LoggerFactory.getLogger(ReplicationFactorChangeListener.class);

    @Override
    public void onAlterKeyspace(KeyspaceMetadata before, KeyspaceMetadata after)
    {
        if (!AutoRepairService.instance.getAutoRepairConfig().isAutoRepairSchedulingEnabled())
        {
            logger.debug("Auto-repair is disabled, skipping RF change detection");
            return;
        }

        RFChangeAnalysis analysis = analyzeRFChange(before, after);
        if (analysis.hasChanged)
        {
            logger.info("Detected RF change for keyspace {}: {}", after.name, analysis);
            scheduleRepairForRFChange(after.name, analysis);
        }
    }

    private RFChangeAnalysis analyzeRFChange(KeyspaceMetadata before, KeyspaceMetadata after)
    {
        RFChangeAnalysis analysis = new RFChangeAnalysis();
        
        if (before.params.replication.klass.equals(after.params.replication.klass))
        {
            if (after.params.replication.klass.equals(NetworkTopologyStrategy.class))
            {
                analysis = analyzeNTSChange(before.params.replication.options, after.params.replication.options);
            }
            else if (after.params.replication.klass.equals(SimpleStrategy.class))
            {
                analysis = analyzeSimpleStrategyChange(before.params.replication.options, after.params.replication.options);
            }
        }
        else
        {
            // Strategy class changed - treat as RF change
            analysis.hasChanged = true;
            analysis.changeType = RFChangeType.STRATEGY_CHANGE;
        }
        
        return analysis;
    }

    private RFChangeAnalysis analyzeNTSChange(Map<String, String> beforeOptions, Map<String, String> afterOptions)
    {
        RFChangeAnalysis analysis = new RFChangeAnalysis();
        
        Set<String> beforeDCs = beforeOptions.keySet();
        Set<String> afterDCs = afterOptions.keySet();
        
        // Check for new DCs
        for (String dc : afterDCs)
        {
            if (!beforeDCs.contains(dc))
            {
                analysis.hasChanged = true;
                analysis.changeType = RFChangeType.NEW_DC_ADDED;
                analysis.newDatacenters.add(dc);
            }
        }
        
        // Check for RF changes in existing DCs
        for (String dc : beforeDCs)
        {
            if (afterDCs.contains(dc))
            {
                ReplicationFactor beforeRF = ReplicationFactor.fromString(beforeOptions.get(dc));
                ReplicationFactor afterRF = ReplicationFactor.fromString(afterOptions.get(dc));
                
                if (beforeRF.allReplicas != afterRF.allReplicas)
                {
                    analysis.hasChanged = true;
                    if (afterRF.allReplicas > beforeRF.allReplicas)
                    {
                        analysis.changeType = RFChangeType.RF_INCREASED;
                    }
                    else
                    {
                        analysis.changeType = RFChangeType.RF_DECREASED;
                    }
                }
            }
        }
        
        return analysis;
    }

    private RFChangeAnalysis analyzeSimpleStrategyChange(Map<String, String> beforeOptions, Map<String, String> afterOptions)
    {
        RFChangeAnalysis analysis = new RFChangeAnalysis();
        
        String beforeRFStr = beforeOptions.get("replication_factor");
        String afterRFStr = afterOptions.get("replication_factor");
        
        if (beforeRFStr != null && afterRFStr != null)
        {
            int beforeRF = Integer.parseInt(beforeRFStr);
            int afterRF = Integer.parseInt(afterRFStr);
            
            if (beforeRF != afterRF)
            {
                analysis.hasChanged = true;
                analysis.changeType = afterRF > beforeRF ? RFChangeType.RF_INCREASED : RFChangeType.RF_DECREASED;
            }
        }
        
        return analysis;
    }

    private void scheduleRepairForRFChange(String keyspaceName, RFChangeAnalysis analysis)
    {
        switch (analysis.changeType)
        {
            case NEW_DC_ADDED:
                logger.info("New DC added to keyspace {}, consider running rebuild instead of repair", keyspaceName);
                // For new DC, rebuild is more efficient than repair, but we'll still schedule repair as fallback
                scheduleFullRepair(keyspaceName, "New datacenter added");
                break;
                
            case RF_INCREASED:
                logger.info("RF increased for keyspace {}, scheduling full repair", keyspaceName);
                scheduleFullRepair(keyspaceName, "Replication factor increased");
                break;
                
            case RF_DECREASED:
                logger.info("RF decreased for keyspace {}, scheduling full repair. Consider running cleanup manually.", keyspaceName);
                scheduleFullRepair(keyspaceName, "Replication factor decreased");
                break;
                
            case STRATEGY_CHANGE:
                logger.info("Replication strategy changed for keyspace {}, scheduling full repair", keyspaceName);
                scheduleFullRepair(keyspaceName, "Replication strategy changed");
                break;
        }
    }

    private void scheduleFullRepair(String keyspaceName, String reason)
    {
        // Force repair for all nodes to ensure consistency after RF change
        AutoRepairUtils.setForceRepairForKeyspace(AutoRepairConfig.RepairType.FULL, keyspaceName, reason);
    }

    private static class RFChangeAnalysis
    {
        boolean hasChanged = false;
        RFChangeType changeType;
        Set<String> newDatacenters = new java.util.HashSet<>();
        
        @Override
        public String toString()
        {
            return String.format("RFChangeAnalysis{hasChanged=%s, changeType=%s, newDatacenters=%s}", 
                               hasChanged, changeType, newDatacenters);
        }
    }

    private enum RFChangeType
    {
        RF_INCREASED,
        RF_DECREASED,
        NEW_DC_ADDED,
        STRATEGY_CHANGE
    }
}