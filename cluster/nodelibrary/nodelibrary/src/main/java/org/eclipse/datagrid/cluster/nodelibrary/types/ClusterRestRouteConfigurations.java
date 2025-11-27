package org.eclipse.datagrid.cluster.nodelibrary.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
 * %%
 * Copyright (C) 2025 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

public final class ClusterRestRouteConfigurations
{
    public static final class MediaTypes
    {
        private static final String WILDCARD = "*/*";
        private static final String APPLICATION_JSON = "application/json";
        private static final String APPLICATION_OCTET_STREAM = "application/octet-stream";
        private static final String TEXT_PLAIN = "text/plain";

        private MediaTypes()
        {
        }
    }

    public static final String ROOT_PATH = "/eclipse-datagrid-cluster-controller";

    public static final class GetMicrostreamDistributor
    {
        public static final String PATH = "/eclipse-datagrid-distributor";
        public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

        private GetMicrostreamDistributor()
        {
        }
    }

    public static final class PostMicrostreamActivateDistributorStart
    {
        public static final String PATH = "/eclipse-datagrid-activate-distributor/start";
        public static final String CONSUMES = MediaTypes.WILDCARD;
        public static final String PRODUCES = MediaTypes.WILDCARD;

        private PostMicrostreamActivateDistributorStart()
        {
        }
    }

    public static final class PostMicrostreamActivateDistributorFinish
    {
        public static final String PATH = "/eclipse-datagrid-activate-distributor/finish";
        public static final String CONSUMES = MediaTypes.WILDCARD;
        public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

        private PostMicrostreamActivateDistributorFinish()
        {
        }
    }

    public static final class GetMicrostreamHealth
    {
        public static final String PATH = "/eclipse-datagrid-health";
        public static final String PRODUCES = MediaTypes.WILDCARD;

        private GetMicrostreamHealth()
        {
        }
    }

    public static final class GetMicrostreamHealthReady
    {
        public static final String PATH = "/eclipse-datagrid-health/ready";
        public static final String PRODUCES = MediaTypes.WILDCARD;

        private GetMicrostreamHealthReady()
        {
        }
    }

    public static final class GetMicrostreamStorageBytes
    {
        public static final String PATH = "/eclipse-datagrid-storage-bytes";
        public static final String PRODUCES = MediaTypes.TEXT_PLAIN;

        private GetMicrostreamStorageBytes()
        {
        }
    }

    public static final class PostMicrostreamBackup
    {
        public static final String PATH = "/eclipse-datagrid-backup";
        public static final String CONSUMES = MediaTypes.APPLICATION_JSON;
        public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

        public static final class Body
        {
            private Boolean useManualSlot;

            public Boolean getUseManualSlot()
            {
                return this.useManualSlot;
            }

            public void setUseManualSlot(final Boolean useManualSlot)
            {
                this.useManualSlot = useManualSlot;
            }
        }

        private PostMicrostreamBackup()
        {
        }
    }

    public static final class GetMicrostreamBackup
    {
        public static final String PATH = "/eclipse-datagrid-backup";
        public static final String PRODUCES = MediaTypes.APPLICATION_JSON;


        private GetMicrostreamBackup()
        {
        }
    }

    public static final class PostMicrostreamUpdates
    {
        public static final String PATH = "/eclipse-datagrid-updates";
        public static final String CONSUMES = MediaTypes.WILDCARD;
        public static final String PRODUCES = MediaTypes.WILDCARD;

        private PostMicrostreamUpdates()
        {
        }
    }

    public static final class GetMicrostreamUpdates
    {
        public static final String PATH = "/eclipse-datagrid-updates";
        public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

        private GetMicrostreamUpdates()
        {
        }
    }

    public static final class PostMicrostreamResumeUpdates
    {
        public static final String PATH = "/eclipse-datagrid-resume-updates";
        public static final String CONSUMES = MediaTypes.WILDCARD;
        public static final String PRODUCES = MediaTypes.WILDCARD;

        private PostMicrostreamResumeUpdates()
        {
        }
    }

    public static final class PostMicrostreamGc
    {
        public static final String PATH = "/eclipse-datagrid-gc";
        public static final String CONSUMES = MediaTypes.WILDCARD;
        public static final String PRODUCES = MediaTypes.WILDCARD;

        private PostMicrostreamGc()
        {
        }
    }

    public static final class GetMicrostreamGc
    {
        public static final String PATH = "/eclipse-datagrid-gc";
        public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

        private GetMicrostreamGc()
        {
        }
    }

    private ClusterRestRouteConfigurations()
    {
    }
}
