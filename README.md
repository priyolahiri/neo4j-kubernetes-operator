# Neo4j Enterprise Operator for Kubernetes

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Report Card](https://goreportcard.com/badge/github.com/neo4j-labs/neo4j-operator)](https://goreportcard.com/report/github.com/neo4j-labs/neo4j-operator)
[![GitHub Release](https://img.shields.io/github/release/neo4j-labs/neo4j-operator.svg)](https://github.com/neo4j-labs/neo4j-operator/releases)
[![Enterprise Only](https://img.shields.io/badge/Neo4j-Enterprise%20Only-red.svg)](https://neo4j.com/enterprise)
[![Min Version](https://img.shields.io/badge/Neo4j-5.26%2B-blue.svg)](https://neo4j.com/docs)

> 🏢 **ENTERPRISE EDITION ONLY**: This operator exclusively supports Neo4j Enterprise Edition 5.26 and above. Community Edition is NOT supported.

The Neo4j Enterprise Operator for Kubernetes provides a complete solution for deploying, managing, and scaling Neo4j Enterprise clusters in Kubernetes environments. Built with Kubernetes best practices, it offers enterprise-grade features including high availability, automated backups, security, and comprehensive observability.

## 🤔 What is Neo4j and Why Use This Operator?

**Neo4j** is a leading graph database that stores data as nodes and relationships, making it ideal for applications that need to understand complex connections in data (like social networks, fraud detection, recommendation engines, and knowledge graphs).

**This Kubernetes Operator** automates the deployment and management of Neo4j clusters on Kubernetes, handling:
- Cluster setup and scaling
- High availability and failover
- Automated backups and recovery
- Security and access control
- Monitoring and maintenance

**Perfect for**: Data engineers, DevOps teams, and developers who want to run Neo4j in production without managing the operational complexity.

## ⚠️ Important Version Requirements

**This operator has been specifically designed and configured to work ONLY with Neo4j Enterprise Edition 5.26 and newer versions.**

| Edition | Version | Support Status |
|---------|---------|----------------|
| Neo4j Community | Any | ❌ **NOT SUPPORTED** |
| Neo4j Enterprise | < 5.26 | ❌ **NOT SUPPORTED** |
| Neo4j Enterprise | 5.26+ | ✅ **FULLY SUPPORTED** |

The operator includes runtime validation to ensure only supported Neo4j versions are used.

## 🚀 Getting Started

### 📋 Prerequisites

Before you begin, ensure you have:

#### For beginners:
1. **A Kubernetes cluster** (we'll help you create one if needed)
2. **kubectl** command-line tool installed and configured
3. **Basic understanding** of YAML files (we'll explain as we go)

#### For advanced users:
- Kubernetes cluster (v1.20+) with sufficient resources
- kubectl configured with cluster-admin permissions
- Helm 3.x (optional but recommended)
- Understanding of Kubernetes concepts (Pods, Services, StatefulSets)

### 🆘 Don't Have Kubernetes Yet?

**No problem!** Here are quick ways to get started:

#### Option 1: Local Development (Easiest)
```bash
# Install kind (Kubernetes in Docker)
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.20.0/kind-linux-amd64
chmod +x ./kind
sudo mv ./kind /usr/local/bin/kind

# Create a local cluster
kind create cluster --name neo4j-test

# Verify it's working
kubectl cluster-info
```

#### Option 2: Cloud Providers
- **AWS**: [Amazon EKS](https://aws.amazon.com/eks/getting-started/)
- **Google Cloud**: [Google GKE](https://cloud.google.com/kubernetes-engine/docs/quickstart)
- **Azure**: [Azure AKS](https://docs.microsoft.com/en-us/azure/aks/kubernetes-walkthrough)
- **DigitalOcean**: [DigitalOcean Kubernetes](https://www.digitalocean.com/products/kubernetes/)

### 🎯 Quick Start (5 Minutes)

#### Step 1: Install the Neo4j Operator

Choose your preferred method:

**🟢 Beginner-Friendly (kubectl)**
```bash
# This installs the operator that will manage Neo4j for you
kubectl apply -f https://github.com/neo4j-labs/neo4j-operator/releases/latest/download/neo4j-operator.yaml

# Wait for the operator to be ready (this may take 1-2 minutes)
kubectl wait --for=condition=available deployment/neo4j-operator-controller-manager -n neo4j-operator-system --timeout=300s

# Verify installation
kubectl get pods -n neo4j-operator-system
```

**🔧 Advanced (Helm)**
```bash
# Add the Neo4j operator Helm repository
helm repo add neo4j-operator https://neo4j-labs.github.io/neo4j-operator
helm repo update

# Install with custom configuration
helm install neo4j-operator neo4j-operator/neo4j-operator \
  --namespace neo4j-operator-system \
  --create-namespace \
  --set operator.watchNamespace="" \
  --set operator.resources.limits.memory=512Mi
```

#### Step 2: Create Your First Neo4j Cluster

**🟢 Simple Example (Good for learning)**

Create a file called `my-first-neo4j.yaml`:

```yaml
# This tells Kubernetes we want to create a Neo4j cluster
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: my-first-neo4j          # Name of your cluster
  namespace: default            # Kubernetes namespace
spec:
  # Neo4j Docker image to use
  image:
    repo: neo4j
    tag: "5.26-enterprise"      # Must be Enterprise edition
  
  # Cluster configuration
  topology:
    primaries: 1                # Start with 1 main database instance
    secondaries: 0              # No read replicas for now
  
  # Storage (where your data will be saved)
  storage:
    className: "standard"       # Use default storage class
    size: "10Gi"               # 10GB should be enough for testing
  
  # Authentication (how to log in)
  auth:
    provider: native            # Use Neo4j's built-in authentication
    secretRef: neo4j-auth-secret
```

Create the authentication secret:
```bash
# Create a secret with Neo4j username and password
kubectl create secret generic neo4j-auth-secret \
  --from-literal=username=neo4j \
  --from-literal=password=mySecurePassword123
```

Deploy your cluster:
```bash
# Create the Neo4j cluster
kubectl apply -f my-first-neo4j.yaml

# Watch it start up (this takes 2-5 minutes)
kubectl get neo4jenterprisecluster my-first-neo4j -w
```

#### Step 3: Access Your Neo4j Database

```bash
# Set up port forwarding to access Neo4j locally
kubectl port-forward service/my-first-neo4j-client 7474:7474 7687:7687

# Now open your browser to: http://localhost:7474
# Login with: username=neo4j, password=mySecurePassword123
```

🎉 **Congratulations!** You now have a running Neo4j Enterprise cluster!

## 📊 Understanding What You Just Created

Your Neo4j cluster includes several Kubernetes resources:

```text
📦 my-first-neo4j (Neo4jEnterpriseCluster)
├── 🏛️  StatefulSet (manages the database pods)
│   └── 📱 Pod (runs the actual Neo4j database)
├── 🌐 Services (network access to your database)
│   ├── my-first-neo4j-client (for applications)
│   └── my-first-neo4j-headless (internal clustering)
├── 📋 ConfigMap (Neo4j configuration)
├── 🔒 Secret (authentication credentials)
└── 💾 PersistentVolume (your data storage)
```

**What each component does:**
- **StatefulSet**: Ensures your database stays running and restarts if it crashes
- **Services**: Provide network endpoints for accessing your database
- **ConfigMap**: Stores Neo4j configuration settings
- **Secret**: Safely stores passwords and certificates
- **PersistentVolume**: Stores your actual graph data (survives pod restarts)

## 📋 Table of Contents

- [Features](#-features)
- [Architecture](#-architecture)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Custom Resources](#-custom-resources)
- [Advanced Deployments](#-advanced-deployments)
- [Security](#-security)
- [Monitoring & Observability](#-monitoring--observability)
- [Backup & Recovery](#-backup--recovery)
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)

## ✨ Features

### Core Capabilities
- **Multi-replica High Availability**: Deploy Neo4j clusters with configurable core and read replica instances
- **Topology-Aware Placement**: Automatically distribute primaries across availability zones to prevent quorum loss
- **Automated Operations**: Self-healing, rolling updates, and automated scaling
- **Enterprise Security**: TLS encryption, authentication, authorization, and RBAC integration
- **Cloud-Native Storage**: Persistent volumes with automatic provisioning and backup
- **Multi-Database Support**: Create and manage multiple databases within a cluster

### Enterprise Features
- **Automated Backups**: Scheduled backups to cloud storage (AWS S3, GCS, Azure Blob)
- **Point-in-Time Recovery**: Restore databases to specific timestamps
- **User & Role Management**: Declarative user, role, and privilege management
- **Cluster Federation**: Multi-cluster deployments across regions
- **Advanced Monitoring**: Prometheus metrics, alerts, and grafana dashboards

## 🏗️ Architecture

The operator manages multiple custom resources to provide a complete Neo4j platform:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Neo4jEnterpriseCluster  │    │  Neo4jDatabase  │    │   Neo4jBackup   │
│                 │    │                 │    │                 │
│ • Cluster Spec  │    │ • Database Spec │    │ • Backup Config │
│ • Replicas      │    │ • Initial Data  │    │ • Schedule      │
│ • Resources     │    │ • Options       │    │ • Retention     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│    Neo4jUser    │    │    Neo4jRole    │    │   Neo4jGrant    │
│                 │    │                 │    │                 │
│ • Username      │    │ • Role Name     │    │ • Privileges    │
│ • Password Ref  │    │ • Privileges    │    │ • Target        │
│ • Roles         │    │ • Description   │    │ • Statements    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📦 Installation

### System Requirements

- **Kubernetes**: v1.20+
- **Memory**: Minimum 4GB per Neo4j instance
- **CPU**: Minimum 2 cores per Neo4j instance
- **Storage**: Persistent volumes with ReadWriteOnce access
- **Network**: ClusterIP or LoadBalancer service support

### Installation Methods

#### Method 1: Direct Installation

```bash
# Install CRDs and operator
kubectl apply -f https://github.com/neo4j-labs/neo4j-operator/releases/latest/download/neo4j-operator.yaml

# Verify installation
kubectl get pods -n neo4j-operator-system
```

#### Method 2: Helm Installation

```bash
# Add repository
helm repo add neo4j-operator https://neo4j-labs.github.io/neo4j-operator
helm repo update

# Install operator
helm install neo4j-operator neo4j-operator/neo4j-operator \
  --namespace neo4j-operator-system \
  --create-namespace

# Customize installation
helm install neo4j-operator neo4j-operator/neo4j-operator \
  --namespace neo4j-operator-system \
  --create-namespace \
  --set operator.watchNamespace=neo4j \
  --set operator.resources.limits.memory=512Mi
```

#### Method 3: From Source

```bash
# Clone repository
git clone https://github.com/neo4j-labs/neo4j-operator.git
cd neo4j-operator

# Install CRDs
make install

# Deploy operator
make deploy IMG=your-registry/neo4j-operator:latest
```

## ⚙️ Configuration

### Basic Cluster Configuration

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: neo4j-cluster
spec:
  image:
    repo: neo4j
    tag: "5.26-enterprise"
  topology:
    primaries: 3
    secondaries: 2
  storage:
    className: "fast-ssd"
    size: "100Gi"
  tls:
    mode: cert-manager
    issuerRef:
      name: letsencrypt-prod
      kind: ClusterIssuer
  auth:
    provider: native
    secretRef: neo4j-auth-secret
  service:
    ingress:
      enabled: true
      className: nginx
      annotations:
        nginx.ingress.kubernetes.io/ssl-redirect: "true"
  backups:
    defaultStorage:
      type: s3
      bucket: neo4j-backups
      path: /cluster-backups
    cloud:
      provider: aws
      identity:
        provider: aws
        autoCreate:
          enabled: true
          annotations:
            eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/Neo4jBackupRole
```

### Topology-Aware Placement

**🎯 NEW FEATURE**: Automatically distribute Neo4j primaries across availability zones to prevent quorum loss and ensure high availability.

#### Basic Topology-Aware Configuration

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: neo4j-ha-cluster
spec:
  image:
    repo: neo4j
    tag: "5.26-enterprise"
  topology:
    primaries: 3
    secondaries: 3
    
    # Enable automatic distribution across zones
    enforceDistribution: true
    
    # Advanced placement configuration
    placement:
      # Topology spread constraints
      topologySpread:
        enabled: true
        topologyKey: "topology.kubernetes.io/zone"
        maxSkew: 1  # Perfect distribution
        whenUnsatisfiable: "DoNotSchedule"  # Hard constraint
      
      # Pod anti-affinity to prevent co-location
      antiAffinity:
        enabled: true
        type: "required"  # Hard anti-affinity
```

**Benefits:**
- ✅ **Automatic zone discovery** - No manual configuration needed
- ✅ **Quorum protection** - Prevents single-zone failures from causing downtime
- ✅ **Set and forget** - Operator handles all placement logic automatically
- ✅ **Flexible constraints** - Support for both hard and soft placement requirements

For detailed topology configuration options, see the [Topology-Aware Placement Guide](docs/topology-aware-placement.md).

### Advanced Configuration Options

#### High Availability Setup

```yaml
spec:
  # Cluster configuration
  cluster:
    minimumClusterSize: 3
    multiDCPolicy: "require_all"
    
  # Pod placement
  podSpec:
    affinity:
      podAntiAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
        - labelSelector:
            matchLabels:
              app: neo4j
          topologyKey: kubernetes.io/hostname
  
  # Service mesh integration
  serviceMesh:
    enabled: true
    mtls: true
```

#### Security Configuration

```yaml
spec:
  # Authentication
  auth:
    disabled: false
    adminPasswordSecret:
      name: neo4j-admin-secret
      key: password
  
  # TLS configuration
  tls:
    enabled: true
    certificate:
      issuerRef:
        name: ca-issuer
        kind: ClusterIssuer
  
  # LDAP integration
  ldap:
    enabled: true
    host: "ldap.company.com"
    userDNPattern: "cn={0},ou=users,dc=company,dc=com"
```

## 📚 Custom Resources

### Neo4jEnterpriseCluster

The primary resource for deploying Neo4j clusters.

**Key Features:**
- Configurable cluster topology (core + read replicas)
- Resource management and autoscaling
- Rolling updates and automated recovery
- Service exposure and ingress configuration

**Example:**
```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: neo4j-cluster
spec:
  image:
    repo: neo4j
    tag: "5.26-enterprise"
  topology:
    primaries: 3
    secondaries: 2
  storage:
    className: "fast-ssd"
    size: "100Gi"
  tls:
    mode: cert-manager
    issuerRef:
      name: letsencrypt-prod
      kind: ClusterIssuer
  auth:
    provider: native
    secretRef: neo4j-auth-secret
  service:
    ingress:
      enabled: true
      className: nginx
      annotations:
        nginx.ingress.kubernetes.io/ssl-redirect: "true"
  backups:
    defaultStorage:
      type: s3
      bucket: neo4j-backups
      path: /cluster-backups
    cloud:
      provider: aws
      identity:
        provider: aws
        autoCreate:
          enabled: true
          annotations:
            eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/Neo4jBackupRole
```

### Neo4jDatabase

Manages individual databases within a Neo4j cluster.

**Key Features:**
- Database lifecycle management
- Initial data population
- Configuration management
- Access control integration

**Example:**
```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jDatabase
metadata:
  name: analytics-db
spec:
  clusterRef: neo4j-cluster
  
  # Database options
  options:
    topology: "primary"
    seedURI: "neo4j://seed-cluster:7687"
  
  # Initial data
  initialData:
    source: cypher
    configMapRef: analytics-init-data
```

### Neo4jBackup

Configures automated backup schedules.

**Key Features:**
- Scheduled backups with cron expressions
- Multiple storage backends (S3, GCS, Azure)
- Retention policies
- Incremental and full backup support

**Example:**
```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jBackup
metadata:
  name: daily-backup
spec:
  clusterRef: neo4j-cluster
  
  # Schedule (daily at 2 AM)
  schedule: "0 2 * * *"
  
  # Storage configuration
  storage:
    s3:
      bucket: neo4j-backups
      prefix: "production/"
      region: us-west-2
      credentialsSecret:
        name: aws-credentials
  
  # Retention
  retention:
    keepDaily: 7
    keepWeekly: 4
    keepMonthly: 12
```

### Neo4jUser

Manages Neo4j user accounts.

**Key Features:**
- Declarative user management
- Password rotation
- Role assignment
- Account lifecycle management

**Example:**
```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jUser
metadata:
  name: analytics-user
spec:
  clusterRef: neo4j-cluster
  username: analytics
  
  # Password from secret
  passwordSecret:
    name: analytics-password
    key: password
  
  # Role assignments
  roles:
    - reader
    - analyst
  
  # User properties
  properties:
    email: "analytics@company.com"
    department: "data-science"
```

### Neo4jRole

Defines custom roles with specific privileges.

**Example:**
```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jRole
metadata:
  name: data-scientist
spec:
  clusterRef: neo4j-cluster
  description: "Role for data science team"
  
  # Privilege definitions
  privileges:
    - action: GRANT
      privilege: "READ"
      resource: "database:analytics"
    - action: GRANT
      privilege: "MATCH"
      resource: "graph:*"
      qualifier: "Label:*"
```

### Neo4jGrant

Manages fine-grained privilege assignments.

**Example:**
```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jGrant
metadata:
  name: analytics-permissions
spec:
  clusterRef: neo4j-cluster
  
  # Target user or role
  target:
    kind: User
    name: analytics
  
  # Privilege statements
  statements:
    - "GRANT READ ON DATABASE analytics TO analytics"
    - "GRANT MATCH {*} ON GRAPH analytics TO analytics"
```

## 🚀 Advanced Deployments

### Multi-Region Setup

Deploy Neo4j across multiple regions for disaster recovery:

```yaml
# Primary region cluster
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: neo4j-primary
  namespace: neo4j-us-west
spec:
  image:
    repo: neo4j
    tag: "5.26-enterprise"
  topology:
    primaries: 3
    secondaries: 2
  
  # Multi-DC configuration
  cluster:
    multiDCPolicy: "require_majority"
    seedList:
      - "neo4j-primary-0.neo4j-primary:7687"
      - "neo4j-secondary-0.neo4j-secondary.neo4j-eu:7687"
  
  # Region-specific configuration
  config:
    causal_clustering.multi_dc_license: "true"
    causal_clustering.server_groups: "us-west"
```

### Disaster Recovery Setup

```yaml
# Backup configuration for DR
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jBackup
metadata:
  name: dr-backup
spec:
  clusterRef: neo4j-primary
  
  # Frequent backups for DR
  schedule: "*/30 * * * *"  # Every 30 minutes
  
  # Cross-region storage
  storage:
    s3:
      bucket: neo4j-dr-backups
      region: us-east-1  # Different region
  
  # DR-specific retention
  retention:
    keepHourly: 48
    keepDaily: 30
```

### Autoscaling Configuration

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: autoscaling-cluster
spec:
  # Base configuration
  image:
    repo: neo4j
    tag: "5.26-enterprise"
  topology:
    primaries: 3
    secondaries: 2
  
  # Autoscaling settings
  autoscaling:
    enabled: true
    minReplicas: 3
    maxReplicas: 10
    minReadReplicas: 1
    maxReadReplicas: 20
    
    # Scaling triggers
    metrics:
      - type: Resource
        resource:
          name: cpu
          target:
            type: Utilization
            averageUtilization: 70
      - type: Resource
        resource:
          name: memory
          target:
            type: Utilization
            averageUtilization: 80
```

## 🔒 Security

### Authentication Methods

#### Basic Authentication
```yaml
spec:
  auth:
    disabled: false
    adminPasswordSecret:
      name: neo4j-admin
      key: password
```

#### LDAP Authentication
```yaml
spec:
  auth:
    ldap:
      enabled: true
      host: "ldap.company.com"
      port: 389
      userDNPattern: "uid={0},ou=people,dc=company,dc=com"
      userSearchBase: "ou=people,dc=company,dc=com"
      userSearchFilter: "(uid={0})"
      groupSearchBase: "ou=groups,dc=company,dc=com"
```

#### OIDC/SSO Integration
```yaml
spec:
  auth:
    oidc:
      enabled: true
      issuerURL: "https://auth.company.com"
      clientId: "neo4j-cluster"
      clientSecretRef:
        name: oidc-secret
        key: client-secret
```

### TLS Configuration

#### Automatic Certificate Management
```yaml
spec:
  tls:
    enabled: true
    # Automatic certificate provisioning via cert-manager
    certificate:
      issuerRef:
        name: letsencrypt-prod
        kind: ClusterIssuer
      dnsNames:
        - neo4j.company.com
```

#### Custom Certificates
```yaml
spec:
  tls:
    enabled: true
    # Custom certificate from secret
    certificateSecret:
      name: neo4j-tls-cert
      certKey: tls.crt
      keyKey: tls.key
      caKey: ca.crt
```

### Network Policies

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: neo4j-network-policy
spec:
  podSelector:
    matchLabels:
      app: neo4j
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: applications
    ports:
    - protocol: TCP
      port: 7687
  - from:
    - namespaceSelector:
        matchLabels:
          name: monitoring
    ports:
    - protocol: TCP
      port: 2004
```

## 📊 Monitoring & Observability

### Prometheus Metrics

The operator exposes comprehensive metrics for monitoring:

```yaml
# ServiceMonitor for Prometheus
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: neo4j-cluster-metrics
spec:
  selector:
    matchLabels:
      app: neo4j
  endpoints:
  - port: metrics
    interval: 30s
    path: /metrics
```

**Key Metrics:**
- `neo4j_cluster_members_total`: Total cluster members
- `neo4j_database_query_execution_time`: Query execution times
- `neo4j_database_store_size_bytes`: Database storage usage
- `neo4j_cluster_read_replicas_available`: Available read replicas

### Alerting Rules

```yaml
groups:
- name: neo4j.rules
  rules:
  - alert: Neo4jEnterpriseClusterDown
    expr: neo4j_cluster_members_available < 2
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "Neo4j cluster has insufficient members"
      
  - alert: Neo4jHighQueryLatency
    expr: neo4j_database_query_execution_time_p95 > 5000
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "High query latency detected"
```

### Grafana Dashboard

Install pre-built dashboards:

```bash
# Add Grafana dashboards
kubectl create configmap neo4j-dashboard \
  --from-file=https://raw.githubusercontent.com/neo4j-labs/neo4j-operator/main/monitoring/grafana-dashboard.json
```

**Dashboard Features:**
- Cluster health and topology visualization
- Query performance metrics
- Resource utilization tracking
- Backup status monitoring

## 💾 Backup & Recovery

### Automated Backups

#### S3 Backup Configuration
```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jBackup
metadata:
  name: s3-backup
spec:
  clusterRef: neo4j-cluster
  
  # Daily backup at 2 AM
  schedule: "0 2 * * *"
  
  storage:
    s3:
      bucket: neo4j-backups
      prefix: "cluster-backups/"
      region: us-west-2
      
      # AWS credentials
      credentialsSecret:
        name: aws-backup-credentials
        accessKeyID: access-key-id
        secretAccessKey: secret-access-key
  
  # Retention policy
  retention:
    keepDaily: 7
    keepWeekly: 4
    keepMonthly: 12
    keepYearly: 3
```

#### Google Cloud Storage
```yaml
spec:
  storage:
    gcs:
      bucket: neo4j-backups
      prefix: "production/"
      
      # Service account credentials
      credentialsSecret:
        name: gcs-credentials
        key: service-account.json
```

#### Azure Blob Storage
```yaml
spec:
  storage:
    azure:
      container: neo4j-backups
      storageAccount: mystorageaccount
      
      # Azure credentials
      credentialsSecret:
        name: azure-credentials
        accountName: account-name
        accountKey: account-key
```

### Point-in-Time Recovery

Restore from a specific backup:

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jRestore
metadata:
  name: restore-operation
spec:
  clusterRef: neo4j-cluster
  
  # Source backup
  source:
    backupRef: s3-backup
    timestamp: "2024-01-15T10:30:00Z"
  
  # Target database
  targetDatabase: restored-db
  
  # Restore options
  options:
    force: false
    dropExisting: true
```

### Manual Backup Operations

```bash
# Trigger immediate backup
kubectl create job manual-backup --from=cronjob/neo4j-backup

# Check backup status
kubectl describe neo4jbackup s3-backup

# List available backups
kubectl get neo4jbackups -o wide
```

## 🛠️ Troubleshooting Guide

### 🆘 Common Issues and Solutions

#### ❗ **Operator Installation Issues**

**Problem**: Operator pods won't start
```bash
# Check operator status
kubectl get pods -n neo4j-operator-system

# Check operator logs
kubectl logs deployment/neo4j-operator-controller-manager -n neo4j-operator-system

# Common fixes:
# 1. Check RBAC permissions
kubectl auth can-i create statefulsets --as=system:serviceaccount:neo4j-operator-system:neo4j-operator-controller-manager

# 2. Verify CRDs are installed
kubectl get crd | grep neo4j
```

**Problem**: "unable to recognize" error
```bash
# This usually means CRDs aren't installed. Reinstall:
kubectl apply -f https://github.com/neo4j-labs/neo4j-operator/releases/latest/download/neo4j-operator.yaml
```

#### ❗ **Cluster Won't Start**

**Problem**: Neo4j pods stuck in Pending state
```bash
# Check pod events
kubectl describe pod <pod-name> -n <namespace>

# Common causes and fixes:
# 1. Insufficient resources
kubectl describe nodes  # Check node capacity

# 2. Storage class doesn't exist
kubectl get storageclass

# 3. Missing secrets
kubectl get secrets -n <namespace>
```

**Problem**: Neo4j pods crash with OOMKilled
```bash
# Check resource limits
kubectl describe neo4jenterprisecluster <cluster-name> -n <namespace>

# Fix: Increase memory limits
kubectl patch neo4jenterprisecluster <cluster-name> -n <namespace> --type='merge' -p='
spec:
  resources:
    limits:
      memory: "8Gi"  # Increase from current value
'
```

**Problem**: Authentication failures
```bash
# Check if auth secret exists and has correct format
kubectl get secret <auth-secret-name> -n <namespace> -o yaml

# Secret should have 'username' and 'password' keys
# Fix: Recreate secret
kubectl delete secret <auth-secret-name> -n <namespace>
kubectl create secret generic <auth-secret-name> \
  --from-literal=username=neo4j \
  --from-literal=password=<your-password> \
  --namespace=<namespace>
```

#### ❗ **Connectivity Issues**

**Problem**: Cannot connect to Neo4j Browser
```bash
# Check service status
kubectl get services -n <namespace>

# Check if ports are correct
kubectl describe service <cluster-name>-client -n <namespace>

# For port-forward issues:
# Make sure no other process is using the ports
lsof -i :7474  # Check if port 7474 is in use
lsof -i :7687  # Check if port 7687 is in use

# Use different local ports if needed
kubectl port-forward service/<cluster-name>-client 17474:7474 17687:7687 -n <namespace>
```

**Problem**: "Connection refused" errors
```bash
# Check if Neo4j pods are actually running
kubectl get pods -n <namespace> -l app.kubernetes.io/name=<cluster-name>

# Check pod logs for errors
kubectl logs <pod-name> -n <namespace>

# Check Neo4j configuration
kubectl logs <pod-name> -n <namespace> | grep -i error
```

#### ❗ **Performance Issues**

**Problem**: Slow queries or high memory usage
```bash
# Check resource usage
kubectl top pods -n <namespace>

# Check Neo4j metrics (if monitoring is enabled)
kubectl port-forward service/<cluster-name>-metrics 2004:2004 -n <namespace>
curl http://localhost:2004/metrics | grep -i memory

# Fix: Tune Neo4j memory settings
# See Memory Configuration section above
```

**Problem**: Cluster split-brain or synchronization issues
```bash
# Check cluster status
kubectl exec <core-pod-name> -n <namespace> -- cypher-shell -u neo4j -p <password> "SHOW SERVERS;"

# Check logs for cluster communication errors
kubectl logs <pod-name> -n <namespace> | grep -i cluster

# Common fix: Restart problematic pod
kubectl delete pod <pod-name> -n <namespace>  # StatefulSet will recreate it
```

### 🔍 Debugging Commands

#### **Cluster Health Check**
```bash
#!/bin/bash
# Save as check-neo4j-health.sh

NAMESPACE=${1:-default}
CLUSTER_NAME=${2:-neo4j-cluster}

echo "🔍 Checking Neo4j cluster health..."
echo "Namespace: $NAMESPACE"
echo "Cluster: $CLUSTER_NAME"
echo ""

# Check operator
echo "📋 Operator Status:"
kubectl get pods -n neo4j-operator-system

# Check cluster resource
echo -e "\n📊 Cluster Status:"
kubectl get neo4jenterprisecluster $CLUSTER_NAME -n $NAMESPACE -o wide

# Check pods
echo -e "\n🏃 Pod Status:"
kubectl get pods -n $NAMESPACE -l app.kubernetes.io/name=$CLUSTER_NAME

# Check services
echo -e "\n🌐 Service Status:"
kubectl get services -n $NAMESPACE -l app.kubernetes.io/name=$CLUSTER_NAME

# Check storage
echo -e "\n💾 Storage Status:"
kubectl get pvc -n $NAMESPACE -l app.kubernetes.io/name=$CLUSTER_NAME

# Check recent events
echo -e "\n📝 Recent Events:"
kubectl get events -n $NAMESPACE --sort-by=.metadata.creationTimestamp | tail -10
```

#### **Detailed Pod Inspection**
```bash
#!/bin/bash
# Save as inspect-neo4j-pod.sh

POD_NAME=$1
NAMESPACE=${2:-default}

if [ -z "$POD_NAME" ]; then
    echo "Usage: $0 <pod-name> [namespace]"
    exit 1
fi

echo "🔍 Inspecting pod: $POD_NAME"
echo ""

# Pod details
echo "📋 Pod Details:"
kubectl describe pod $POD_NAME -n $NAMESPACE

# Resource usage
echo -e "\n📊 Resource Usage:"
kubectl top pod $POD_NAME -n $NAMESPACE --containers

# Logs
echo -e "\n📝 Recent Logs:"
kubectl logs $POD_NAME -n $NAMESPACE --tail=50

# Config files
echo -e "\n⚙️ Neo4j Configuration:"
kubectl exec $POD_NAME -n $NAMESPACE -- cat /etc/neo4j/neo4j.conf | head -20
```

### 📊 Monitoring and Observability

#### **Built-in Monitoring Setup**

**Enable Prometheus Metrics**:
```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: monitored-cluster
spec:
  # ... other config ...
  monitoring:
    enabled: true
    metricsPort: 2004
    prometheusEnabled: true
  
  # Expose metrics service
  service:
    metrics:
      enabled: true
      port: 2004
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "2004"
        prometheus.io/path: "/metrics"
```

**Deploy Prometheus and Grafana**:
```bash
# Add Prometheus Helm repository
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

# Install Prometheus
helm install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace \
  --set grafana.adminPassword=admin123

# Access Grafana
kubectl port-forward service/prometheus-grafana 3000:80 -n monitoring
# Open http://localhost:3000 (admin/admin123)
```

#### **Key Metrics to Monitor**

**Cluster Health Metrics**:
- `neo4j_database_cluster_member_health` - Member health status
- `neo4j_database_cluster_discovery_cluster_members` - Number of cluster members
- `neo4j_database_cluster_discovery_cluster_member_role` - Member roles

**Performance Metrics**:
- `neo4j_database_pool_total_used` - Connection pool usage
- `neo4j_database_transaction_active` - Active transactions
- `neo4j_jvm_memory_heap_used` - Memory usage
- `neo4j_database_query_execution_time` - Query performance

**Custom Grafana Dashboard**:
```json
{
  "dashboard": {
    "title": "Neo4j Enterprise Cluster",
    "panels": [
      {
        "title": "Cluster Members",
        "type": "stat",
        "targets": [
          {
            "expr": "neo4j_database_cluster_discovery_cluster_members",
            "legendFormat": "Members"
          }
        ]
      },
      {
        "title": "Memory Usage",
        "type": "graph",
        "targets": [
          {
            "expr": "neo4j_jvm_memory_heap_used",
            "legendFormat": "{{instance}}"
          }
        ]
      }
    ]
  }
}
```

#### **Log Analysis**

**Centralized Logging Setup**:
```bash
# Install Fluent Bit for log collection
helm repo add fluent https://fluent.github.io/helm-charts
helm install fluent-bit fluent/fluent-bit \
  --namespace logging \
  --create-namespace \
  --set config.outputs='[OUTPUT]\n    Name stdout\n    Match *'

# Or use built-in Kubernetes logging
kubectl logs -f deployment/<cluster-name> -n <namespace>
```

**Important Log Patterns to Watch**:
```bash
# Error patterns
kubectl logs <pod-name> -n <namespace> | grep -i "error\|exception\|failed"

# Cluster communication
kubectl logs <pod-name> -n <namespace> | grep -i "cluster\|raft\|discovery"

# Performance warnings
kubectl logs <pod-name> -n <namespace> | grep -i "slow\|timeout\|memory"

# Security events
kubectl logs <pod-name> -n <namespace> | grep -i "auth\|login\|security"
```

### 🔔 Alerting Setup

**Sample Prometheus Alert Rules**:
```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: neo4j-alerts
  namespace: monitoring
spec:
  groups:
  - name: neo4j.rules
    rules:
    - alert: Neo4jClusterMemberDown
      expr: neo4j_database_cluster_member_health == 0
      for: 5m
      labels:
        severity: critical
      annotations:
        summary: "Neo4j cluster member is down"
        description: "Neo4j cluster member {{ $labels.instance }} has been down for more than 5 minutes"
    
    - alert: Neo4jHighMemoryUsage
      expr: (neo4j_jvm_memory_heap_used / neo4j_jvm_memory_heap_max) > 0.9
      for: 10m
      labels:
        severity: warning
      annotations:
        summary: "Neo4j high memory usage"
        description: "Neo4j instance {{ $labels.instance }} is using more than 90% of heap memory"
    
    - alert: Neo4jSlowQueries
      expr: rate(neo4j_database_query_execution_time_sum[5m]) > 10
      for: 5m
      labels:
        severity: warning
      annotations:
        summary: "Neo4j experiencing slow queries"
        description: "Neo4j instance {{ $labels.instance }} has high query execution times"
```

### 🎯 Performance Tuning

#### **Memory Optimization**
```yaml
# For transaction-heavy workloads
config:
  "dbms.memory.heap.initial_size": "4G"
  "dbms.memory.heap.max_size": "8G"
  "dbms.memory.pagecache.size": "4G"
  "dbms.tx_state.memory_allocation": "ON_HEAP"
  "dbms.tx_state.max_off_heap_memory": "2G"

# For read-heavy workloads
config:
  "dbms.memory.heap.initial_size": "2G"
  "dbms.memory.heap.max_size": "4G"
  "dbms.memory.pagecache.size": "8G"  # Larger page cache
  "dbms.query_cache_size": "1000"
```

#### **Query Performance**
```yaml
# Enable query logging and optimization
config:
  "dbms.logs.query.enabled": "INFO"
  "dbms.logs.query.threshold": "1s"
  "dbms.logs.query.parameter_logging_enabled": "true"
  "dbms.query_cache_size": "1000"
  "dbms.cypher.min_replan_interval": "10s"
```

#### **Storage Optimization**
```yaml
# For high-write workloads
storage:
  className: "io2"  # Provisioned IOPS
  size: "500Gi"
  annotations:
    ebs.csi.aws.com/iops: "5000"
    ebs.csi.aws.com/type: "io2"

# Additional Neo4j storage settings
config:
  "dbms.checkpoint.interval.time": "15m"
  "dbms.checkpoint.interval.tx": "100000"
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Development Setup

```bash
# Clone the repository
git clone https://github.com/neo4j-labs/neo4j-operator.git
cd neo4j-operator

# Set up development environment
make setup-dev

# Run tests
make test

# Build and run locally
make dev-run
```

### Reporting Issues

Please use the [GitHub Issues](https://github.com/neo4j-labs/neo4j-operator/issues) page to report bugs or request features.

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🔗 Links

- [Neo4j Documentation](https://neo4j.com/docs/)
- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [Operator Pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/)
- [Development Guide](docs/development.md)

---

**Maintained by the Priyo Lahiri Team**

For enterprise support, please contact [Neo4j Professional Services](https://neo4j.com/professional-services/).

## 🎯 Common Use Cases & Examples

### 🏢 Enterprise Use Cases

#### 1. **Fraud Detection System**
```yaml
# High-performance cluster for real-time fraud detection
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: fraud-detection
spec:
  image:
    repo: neo4j
    tag: "5.26-enterprise"
  topology:
    primaries: 3                # High availability
    secondaries: 2              # Read replicas for queries
  resources:
    requests:
      memory: "8Gi"             # More memory for complex queries
      cpu: "4"
    limits:
      memory: "16Gi"
      cpu: "8"
  storage:
    className: "fast-ssd"       # Fast storage for performance
    size: "500Gi"
```

#### 2. **Social Network / Recommendation Engine**
```yaml
# Optimized for traversal queries
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: social-recommendations
spec:
  image:
    repo: neo4j
    tag: "5.26-enterprise"
  topology:
    primaries: 3
    secondaries: 4              # Many read replicas for recommendations
  # Custom Neo4j configuration for recommendations
  config:
    "dbms.memory.heap.initial_size": "4G"
    "dbms.memory.heap.max_size": "8G"
    "dbms.memory.pagecache.size": "2G"
```

#### 3. **Knowledge Graph / Data Lineage**
```yaml
# Multi-database setup for different domains
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: knowledge-graph
spec:
  image:
    repo: neo4j
    tag: "5.26-enterprise"
  topology:
    primaries: 3
    secondaries: 1
  # Enable multiple databases
  config:
    "dbms.databases.default_to_read_only": "false"
    "dbms.databases.read_only": "false"
---
# Create separate databases for different domains
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jDatabase
metadata:
  name: data-lineage-db
spec:
  clusterRef:
    name: knowledge-graph
  name: "lineage"
  initialData:
    cypherScript: |
      // Create schema for data lineage
      CREATE CONSTRAINT unique_dataset IF NOT EXISTS FOR (d:Dataset) REQUIRE d.name IS UNIQUE;
      CREATE CONSTRAINT unique_transformation IF NOT EXISTS FOR (t:Transformation) REQUIRE t.id IS UNIQUE;
```

### 📚 Learning Examples

#### **Example 1: Movie Database (Neo4j Classic)**
Perfect for learning graph concepts:

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jDatabase
metadata:
  name: movie-database
spec:
  clusterRef:
    name: my-first-neo4j
  name: "movies"
  initialData:
    cypherScript: |
      // Create sample movie data
      CREATE (matrix:Movie {title: 'The Matrix', released: 1999})
      CREATE (keanu:Person {name: 'Keanu Reeves', born: 1964})
      CREATE (carrie:Person {name: 'Carrie-Anne Moss', born: 1967})
      CREATE (keanu)-[:ACTED_IN {roles: ['Neo']}]->(matrix)
      CREATE (carrie)-[:ACTED_IN {roles: ['Trinity']}]->(matrix)
```

#### **Example 2: Company Org Chart**
Understanding hierarchical relationships:

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jDatabase
metadata:
  name: org-chart
spec:
  clusterRef:
    name: my-first-neo4j
  name: "orgchart"
  initialData:
    cypherScript: |
      // Create organizational structure
      CREATE (ceo:Employee {name: 'Alice Johnson', title: 'CEO', level: 1})
      CREATE (cto:Employee {name: 'Bob Smith', title: 'CTO', level: 2})
      CREATE (dev1:Employee {name: 'Carol Davis', title: 'Senior Developer', level: 3})
      CREATE (dev2:Employee {name: 'David Wilson', title: 'Developer', level: 3})
      
      CREATE (ceo)-[:MANAGES]->(cto)
      CREATE (cto)-[:MANAGES]->(dev1)
      CREATE (cto)-[:MANAGES]->(dev2)
      CREATE (dev1)-[:MENTORS]->(dev2)
```

## 🚦 Step-by-Step Deployment Guide

### Phase 1: Basic Setup (Start Here)

#### 1.1 **Prepare Your Environment**

```bash
# Check if kubectl is working
kubectl version --client

# Check cluster connectivity
kubectl cluster-info

# Create a namespace for your Neo4j resources (optional but recommended)
kubectl create namespace neo4j-production
```

#### 1.2 **Install Prerequisites**

```bash
# Install cert-manager for TLS certificates (recommended for production)
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.0/cert-manager.yaml

# Wait for cert-manager to be ready
kubectl wait --for=condition=available deployment/cert-manager -n cert-manager --timeout=300s
```

#### 1.3 **Deploy the Operator**

```bash
# Install the Neo4j operator
kubectl apply -f https://github.com/neo4j-labs/neo4j-operator/releases/latest/download/neo4j-operator.yaml

# Verify the operator is running
kubectl get pods -n neo4j-operator-system
```

### Phase 2: Deploy Your First Cluster

#### 2.1 **Create Authentication Secret**

```bash
# Generate a strong password
export NEO4J_PASSWORD=$(openssl rand -base64 32)

# Create the authentication secret
kubectl create secret generic neo4j-auth \
  --from-literal=username=neo4j \
  --from-literal=password=${NEO4J_PASSWORD} \
  --namespace=neo4j-production

# Save the password for later use
echo "Your Neo4j password is: ${NEO4J_PASSWORD}"
```

#### 2.2 **Deploy Basic Production Cluster**

Create `neo4j-production.yaml`:

```yaml
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: neo4j-production
  namespace: neo4j-production
spec:
  # Neo4j version
  image:
    repo: neo4j
    tag: "5.26-enterprise"
  
  # Cluster topology
  topology:
    primaries: 3                # 3 core servers for high availability
    secondaries: 2              # 2 read replicas for read scaling
  
  # Resource allocation
  resources:
    requests:
      memory: "4Gi"             # Minimum memory
      cpu: "2"                  # Minimum CPU
    limits:
      memory: "8Gi"             # Maximum memory
      cpu: "4"                  # Maximum CPU
  
  # Storage configuration
  storage:
    className: "fast-ssd"       # Use fast SSD storage class
    size: "100Gi"               # Storage size per instance
  
  # Security settings
  auth:
    provider: native
    secretRef: neo4j-auth
  
  # TLS encryption
  tls:
    mode: cert-manager
    issuerRef:
      name: letsencrypt-prod
      kind: ClusterIssuer
  
  # Service configuration
  service:
    type: LoadBalancer          # Expose to external traffic
    annotations:
      service.beta.kubernetes.io/aws-load-balancer-type: "nlb"  # AWS Network Load Balancer
  
  # Neo4j-specific configuration
  config:
    "dbms.memory.heap.initial_size": "2G"
    "dbms.memory.heap.max_size": "4G"
    "dbms.memory.pagecache.size": "1G"
    "dbms.logs.query.enabled": "INFO"
    "dbms.query_cache_size": "256"
```

Deploy it:
```bash
kubectl apply -f neo4j-production.yaml
```

#### 2.3 **Monitor Deployment Progress**

```bash
# Watch the cluster status
kubectl get neo4jenterprisecluster neo4j-production -n neo4j-production -w

# Check pod status
kubectl get pods -n neo4j-production

# View events
kubectl get events -n neo4j-production --sort-by=.metadata.creationTimestamp

# Check logs if there are issues
kubectl logs -l app.kubernetes.io/name=neo4j-production -n neo4j-production
```

### Phase 3: Access and Verify

#### 3.1 **Get Connection Information**

```bash
# Get service details
kubectl get services -n neo4j-production

# For LoadBalancer service, get external IP
export EXTERNAL_IP=$(kubectl get service neo4j-production-client -n neo4j-production -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
echo "Neo4j is available at: $EXTERNAL_IP:7474"

# Get the password
export NEO4J_PASSWORD=$(kubectl get secret neo4j-auth -n neo4j-production -o jsonpath='{.data.password}' | base64 -d)
echo "Username: neo4j"
echo "Password: $NEO4J_PASSWORD"
```

#### 3.2 **Connect and Test**

```bash
# For local testing with port-forward
kubectl port-forward service/neo4j-production-client 7474:7474 7687:7687 -n neo4j-production

# Test connection with cypher-shell (if installed)
echo "RETURN 'Hello, Neo4j!' AS greeting" | cypher-shell -a bolt://localhost:7687 -u neo4j -p ${NEO4J_PASSWORD}
```

## 🔧 Configuration Guide

### 📊 Understanding Cluster Topology

**Core Servers (Primaries)**:
- Handle all write operations
- Participate in cluster consensus
- Minimum 3 recommended for production (handles 1 failure)
- Odd numbers work best (3, 5, 7)

**Read Replicas (Secondaries)**:
- Handle read-only queries
- Automatically sync from core servers
- Scale these based on read load
- Can be 0 or more

```yaml
topology:
  primaries: 3      # Always odd number, minimum 3 for production
  secondaries: 2    # Scale based on read requirements
```

### 🧠 Memory and CPU Guidelines

**Memory Sizing**:
```yaml
resources:
  requests:
    memory: "4Gi"     # Minimum for small datasets
  limits:
    memory: "8Gi"     # Maximum the pod can use

# Neo4j heap configuration (should be ~50% of container memory)
config:
  "dbms.memory.heap.initial_size": "2G"
  "dbms.memory.heap.max_size": "4G"
  "dbms.memory.pagecache.size": "2G"    # Remaining memory for cache
```

**CPU Sizing**:
```yaml
resources:
  requests:
    cpu: "2"          # Guaranteed CPU
  limits:
    cpu: "4"          # Maximum CPU burst
```

### 💾 Storage Configuration

**Storage Classes**:
```yaml
storage:
  className: "gp3"           # AWS GP3 SSD
  # className: "pd-ssd"      # Google Cloud SSD
  # className: "managed-premium"  # Azure Premium SSD
  size: "100Gi"              # Size per instance
```

**Performance Storage Options**:
```yaml
# High IOPS for transaction-heavy workloads
storage:
  className: "io2"           # AWS Provisioned IOPS
  size: "500Gi"
  annotations:
    volume.beta.kubernetes.io/storage-provisioner: ebs.csi.aws.com
    ebs.csi.aws.com/iops: "3000"
```

### 📊 Monitoring and Alerting Best Practices

#### **Essential Monitoring Setup**
```yaml
# Enable comprehensive monitoring
spec:
  monitoring:
    enabled: true
    metricsPort: 2004
    prometheusEnabled: true
  
  # Configure detailed logging
  config:
    "dbms.logs.query.enabled": "INFO"
    "dbms.logs.query.threshold": "100ms"  # Log slow queries
    "dbms.logs.security.level": "INFO"
    "dbms.security.audit.enabled": "true"
```

#### **Critical Alerts to Set Up**
- Cluster member down
- High memory usage (>85%)
- Slow queries (>1 second)
- Failed authentication attempts
- Disk space usage (>80%)
- Backup failures

### 🔄 Maintenance and Upgrade Best Practices

#### **Rolling Updates**
```yaml
# Configure rolling update strategy
spec:
  upgradeStrategy:
    strategy: "RollingUpdate"
    maxUnavailable: 1
    autoRollback: true
    autoPauseOnFailure: true
```

#### **Backup Before Major Changes**
```bash
# Always backup before upgrades
kubectl create -f - <<EOF
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jBackup
metadata:
  name: pre-upgrade-backup
spec:
  clusterRef:
    name: production-cluster
  type: "full"
  storage:
    type: "s3"
    config:
      bucket: "neo4j-backups"
      path: "/pre-upgrade-$(date +%Y%m%d)"
EOF
```

### 🎯 Performance Best Practices

#### **Query Optimization**
```cypher
-- Use EXPLAIN and PROFILE for query optimization
EXPLAIN MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, f.name;
PROFILE MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, f.name;

-- Create appropriate indexes
CREATE INDEX person_name IF NOT EXISTS FOR (p:Person) ON (p.name);
CREATE CONSTRAINT person_id IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE;
```

#### **Memory Configuration**
```yaml
# Tune based on workload
config:
  # For read-heavy workloads - more page cache
  "dbms.memory.heap.max_size": "4G"
  "dbms.memory.pagecache.size": "8G"
  
  # For write-heavy workloads - more heap
  "dbms.memory.heap.max_size": "8G"
  "dbms.memory.pagecache.size": "4G"
  
  # Query optimization
  "dbms.query_cache_size": "1000"
  "dbms.cypher.min_replan_interval": "10s"
```

## 🤝 Community and Support

### 📚 Learning Resources

#### **Neo4j Learning**
- 📖 [Neo4j Getting Started Guide](https://neo4j.com/developer/get-started/)
- 🎓 [Neo4j GraphAcademy](https://graphacademy.neo4j.com/) - Free online courses
- 📊 [Cypher Query Language](https://neo4j.com/developer/cypher/) - Query language documentation
- 🎥 [Neo4j YouTube Channel](https://www.youtube.com/neo4j) - Tutorials and webinars

#### **Kubernetes Learning**
- 📖 [Kubernetes Documentation](https://kubernetes.io/docs/)
- 🎓 [Kubernetes Learning Path](https://kubernetes.io/docs/tutorials/)
- 📊 [kubectl Cheat Sheet](https://kubernetes.io/docs/reference/kubectl/cheatsheet/)

#### **Operator Pattern**
- 📖 [Kubernetes Operators](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/)
- 📚 [Operator Hub](https://operatorhub.io/) - Discover other operators

### 💬 Community Support

#### **Get Help**
- 🗨️ [Neo4j Community Forum](https://community.neo4j.com/) - Ask questions and share knowledge
- 💬 [Neo4j Discord](https://discord.gg/neo4j) - Real-time chat with the community
- 📧 [Stack Overflow](https://stackoverflow.com/questions/tagged/neo4j) - Technical Q&A

#### **Contribute**
- 🐛 [Report Issues](https://github.com/neo4j-labs/neo4j-operator/issues) - Bug reports and feature requests
- 💡 [GitHub Discussions](https://github.com/neo4j-labs/neo4j-operator/discussions) - Ideas and questions
- 🔀 [Pull Requests](https://github.com/neo4j-labs/neo4j-operator/pulls) - Contribute code improvements

#### **Enterprise Support**
- 🏢 [Neo4j Professional Services](https://neo4j.com/professional-services/) - Expert consulting
- 📞 [Neo4j Support](https://neo4j.com/support/) - Enterprise support plans
- 🎯 [Solution Architecture](https://neo4j.com/professional-services/solution-architecture/) - Custom solutions

### 🎯 What's Next?

#### **Immediate Next Steps**
1. ✅ **Deploy Your First Cluster** - Follow the Quick Start guide above
2. 📚 **Learn Cypher** - Neo4j's query language
3. 🔍 **Explore Sample Data** - Try the movie database example
4. 📊 **Set Up Monitoring** - Configure Prometheus and Grafana

#### **Advanced Topics**
1. 🏗️ **Multi-Database Setup** - Separate data domains
2. 🔄 **Backup and Recovery** - Implement comprehensive backup strategy
3. 🌐 **Multi-Region Deployment** - Distribute across geographic regions
4. 🔐 **Advanced Security** - LDAP integration, fine-grained access control

#### **Production Readiness Checklist**
- [ ] High availability configuration (3+ core servers)
- [ ] TLS encryption enabled
- [ ] Monitoring and alerting configured
- [ ] Backup strategy implemented
- [ ] Security best practices applied
- [ ] Performance tuning completed
- [ ] Documentation updated for your team

### 📖 Additional Documentation

| Topic | Link | Description |
|-------|------|-------------|
| **API Reference** | [docs/api-reference.md](docs/api-reference.md) | Complete API documentation |
| **Developer Guide** | [docs/development/developer-guide.md](docs/development/developer-guide.md) | Contributing and development |
| **Architecture** | [docs/development/architecture.md](docs/development/architecture.md) | System design and components |
| **Testing Guide** | [docs/development/testing-guide.md](docs/development/testing-guide.md) | Testing strategies and tools |
| **Backup & Restore** | [docs/backup-restore-guide.md](docs/backup-restore-guide.md) | Data protection strategies |
| **Performance** | [docs/performance-optimizations.md](docs/performance-optimizations.md) | Optimization techniques |

---

## 🎉 Ready to Get Started?

**Choose your path:**

### 🟢 **I'm new to Neo4j and Kubernetes**
1. Start with [Don't Have Kubernetes Yet?](#-dont-have-kubernetes-yet) section
2. Follow the [🎯 Quick Start (5 Minutes)](#-quick-start-5-minutes) guide
3. Try the [📚 Learning Examples](#-learning-examples)
4. Join the [💬 Community Support](#-community-support)

### 🟡 **I have Kubernetes but new to Neo4j**
1. Jump to [🚦 Step-by-Step Deployment Guide](#-step-by-step-deployment-guide)
2. Explore [🎯 Common Use Cases & Examples](#-common-use-cases--examples)
3. Set up [📊 Monitoring and Observability](#-monitoring-and-observability)

### 🔴 **I'm ready for production**
1. Review [🏆 Best Practices](#-best-practices)
2. Follow the [Production Deployment](#-production-deployment-best-practices) guide
3. Implement [🛠️ Troubleshooting](#%EF%B8%8F-troubleshooting-guide) scripts
4. Consider [Enterprise Support](#-enterprise-support)

---

**Happy graphing with Neo4j on Kubernetes!** 🚀📊

*For the latest updates and releases, watch this repository and join our community.* 