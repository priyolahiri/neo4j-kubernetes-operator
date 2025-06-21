# Architecture Guide

This document describes the architecture and design principles of the Neo4j Enterprise Operator for Kubernetes.

## Table of Contents

- [Overview](#overview)
- [Architecture Principles](#architecture-principles)
- [System Components](#system-components)
- [Controller Architecture](#controller-architecture)
- [Custom Resource Definitions](#custom-resource-definitions)
- [Security Model](#security-model)
- [Networking](#networking)
- [Storage Architecture](#storage-architecture)
- [Observability](#observability)

## Overview

The Neo4j Enterprise Operator is a Kubernetes-native operator that manages the lifecycle of Neo4j database clusters and related resources. It follows the Operator Pattern, extending the Kubernetes API with custom resources and controllers.

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                       │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐    ┌─────────────────────────────────┐ │
│  │   Neo4j Operator │    │       Custom Resources          │ │
│  │   Manager        │◄──►│  - Neo4jEnterpriseCluster      │ │
│  │                 │    │  - Neo4jDatabase               │ │
│  │  ┌─────────────┐ │    │  - Neo4jBackup                │ │
│  │  │Controllers  │ │    │  - Neo4jRestore               │ │
│  │  │- Cluster    │ │    │  - Neo4jUser                  │ │
│  │  │- Database   │ │    │  - Neo4jRole                  │ │
│  │  │- Backup     │ │    │  - Neo4jGrant                 │ │
│  │  │- Restore    │ │    └─────────────────────────────────┘ │
│  │  │- User       │ │                                      │
│  │  │- Role       │ │    ┌─────────────────────────────────┐ │
│  │  │- Grant      │ │    │        Neo4j Resources          │ │
│  │  └─────────────┘ │    │  - StatefulSets                │ │
│  │                 │    │  - Services                    │ │
│  │  ┌─────────────┐ │    │  - ConfigMaps                 │ │
│  │  │Webhooks     │ │    │  - Secrets                    │ │
│  │  │- Validation │ │    │  - PersistentVolumeClaims     │ │
│  │  │- Mutation   │ │    │  - NetworkPolicies            │ │
│  │  └─────────────┘ │    └─────────────────────────────────┘ │
│  └─────────────────┘                                      │
└─────────────────────────────────────────────────────────────┘
```

## Architecture Principles

### 1. Kubernetes-Native Design
- Follows Kubernetes conventions and patterns
- Uses Custom Resource Definitions (CRDs) to extend the API
- Implements the Controller Pattern for resource management
- Leverages Kubernetes primitives (StatefulSets, Services, etc.)

### 2. Declarative Configuration
- Users declare desired state through custom resources
- Controllers continuously reconcile actual state to desired state
- Configuration changes trigger automatic updates
- Rollback capabilities through resource versioning

### 3. Separation of Concerns
- Each controller manages a specific resource type
- Clear boundaries between different operational aspects
- Modular design allowing independent development and testing
- Plugin architecture for extensibility

### 4. Security First
- RBAC integration with Kubernetes security model
- Secret management for sensitive data
- Network policies for traffic isolation
- Admission controllers for validation and security

### 5. Observability
- Comprehensive metrics and logging
- Health checks and readiness probes
- Distributed tracing for complex operations
- Event-driven architecture for audit trails

## System Components

### Core Manager
The main operator process that hosts all controllers and webhooks.

**Responsibilities:**
- Controller lifecycle management
- Webhook server management
- Leader election coordination
- Metrics and health endpoints

**Configuration:**
- Leader election settings
- Webhook certificate management
- Controller concurrency settings
- Resource limits and requests

### Controllers

#### 1. Neo4jEnterpriseCluster Controller
Manages Neo4j Enterprise clusters with causal clustering.

**Resources Managed:**
- StatefulSets for core and read replica nodes
- Services for cluster communication and client access
- ConfigMaps for Neo4j configuration
- Secrets for authentication and certificates

**Key Features:**
- Automatic cluster formation and discovery
- Rolling updates and scaling operations
- Backup scheduling and management
- Monitoring and alerting integration

#### 2. Neo4jDatabase Controller
Manages individual databases within Neo4j clusters.

**Resources Managed:**
- Database creation and configuration
- Schema management
- Access control policies
- Performance monitoring

#### 3. Backup/Restore Controllers
Handle data protection and disaster recovery.

**Backup Controller:**
- Scheduled backup operations
- Multiple storage backend support
- Backup retention policies
- Backup validation and verification

**Restore Controller:**
- Point-in-time recovery
- Cross-cluster restore operations
- Data migration support
- Rollback capabilities

#### 4. Security Controllers (User, Role, Grant)
Manage authentication, authorization, and access control.

**User Controller:**
- User account lifecycle
- Password management
- Authentication integration

**Role Controller:**
- Role-based access control
- Permission management
- Role inheritance

**Grant Controller:**
- Database access grants
- Fine-grained permissions
- Audit trail

### Webhooks

#### Validation Webhooks
Ensure resource specifications are valid and secure.

**Validations:**
- Resource specification validation
- Security policy enforcement
- Resource quota compliance
- Dependency verification

#### Mutation Webhooks
Automatically modify resources to apply defaults and policies.

**Mutations:**
- Default value injection
- Security label application
- Resource annotation
- Configuration standardization

## Controller Architecture

### Reconciliation Loop

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Watch Events  │───►│  Reconcile      │───►│  Update Status  │
│   (Create,      │    │  - Analyze      │    │  - Conditions   │
│    Update,      │    │  - Plan         │    │  - Phase        │
│    Delete)      │    │  - Execute      │    │  - Observability│
└─────────────────┘    └─────────────────┘    └─────────────────┘
         ▲                        │                        │
         │                        ▼                        │
         │               ┌─────────────────┐               │
         └───────────────│  Requeue Logic  │◄──────────────┘
                         │  - Backoff      │
                         │  - Rate Limit   │
                         │  - Error Handle │
                         └─────────────────┘
```

### Controller Components

#### 1. Manager
- Orchestrates multiple controllers
- Provides shared clients and caches
- Manages leader election
- Handles graceful shutdown

#### 2. Controller
- Implements the reconciliation logic
- Manages work queues and rate limiting
- Handles events and updates
- Provides metrics and logging

#### 3. Reconciler
- Core business logic implementation
- Resource creation and management
- Status updates and conditions
- Error handling and retry logic

#### 4. Predicates
- Event filtering logic
- Reduces unnecessary reconciliations
- Performance optimization
- Resource watching efficiency

## Custom Resource Definitions

### Resource Hierarchy

```
Neo4jEnterpriseCluster (Cluster)
├── Neo4jDatabase (Database)
│   ├── Neo4jUser (User)
│   ├── Neo4jRole (Role)
│   └── Neo4jGrant (Access)
├── Neo4jBackup (Data Protection)
└── Neo4jRestore (Recovery)
```

### Common Patterns

#### 1. Spec-Status Pattern
- `spec`: Desired state declaration
- `status`: Current state and conditions
- `metadata`: Standard Kubernetes metadata

#### 2. Conditions
Standardized status reporting:
- `Ready`: Resource is ready for use
- `Progressing`: Operation in progress
- `Degraded`: Partial failure state
- `Failed`: Operation failed

#### 3. Finalizers
Cleanup coordination:
- Resource deletion protection
- Ordered cleanup operations
- External resource cleanup
- Garbage collection

## Security Model

### Authentication and Authorization

#### Kubernetes RBAC
- Service account-based authentication
- Role-based access control
- Cluster-wide and namespace-scoped permissions
- Resource-specific access controls

#### Neo4j Authentication
- Integration with Kubernetes secrets
- External authentication providers
- Certificate-based authentication
- Token-based access control

### Network Security

#### Network Policies
- Pod-to-pod communication control
- Ingress and egress traffic rules
- Namespace isolation
- Service mesh integration

#### TLS/SSL Configuration
- Certificate management
- Mutual TLS authentication
- Encryption in transit
- Certificate rotation

### Data Security

#### Encryption at Rest
- Volume encryption
- Database-level encryption
- Key management integration
- Compliance requirements

#### Secret Management
- Kubernetes secret integration
- External secret providers
- Secret rotation
- Access auditing

## Networking

### Service Architecture

#### 1. Cluster Service
- LoadBalancer or NodePort for external access
- Internal ClusterIP for inter-cluster communication
- Service discovery and DNS integration
- Load balancing and failover

#### 2. Internal Services
- Core node communication
- Read replica routing
- Backup service endpoints
- Monitoring and metrics

### Service Discovery

#### DNS-Based Discovery
- Kubernetes DNS integration
- Service names and namespaces
- Cross-namespace communication
- External DNS integration

#### Custom Discovery
- Neo4j cluster formation
- Dynamic member discovery
- Health-based routing
- Partition tolerance

## Storage Architecture

### Persistent Storage

#### 1. Data Storage
- StatefulSet with PersistentVolumeClaims
- Storage class configuration
- Volume expansion support
- Backup storage separation

#### 2. Configuration Storage
- ConfigMaps for configuration files
- Secrets for sensitive configuration
- Volume mounts and projections
- Configuration hot-reloading

### Backup Storage

#### Storage Backends
- Object storage (S3, GCS, Azure Blob)
- Network file systems (NFS, CIFS)
- Block storage snapshots
- Multi-region replication

#### Backup Strategies
- Full backups
- Incremental backups
- Point-in-time recovery
- Cross-region backup

## Observability

### Metrics

#### Operator Metrics
- Reconciliation performance
- Resource counts and states
- Error rates and latencies
- Queue depths and processing

#### Neo4j Metrics
- Database performance metrics
- Cluster health indicators
- Query performance statistics
- Resource utilization

### Logging

#### Structured Logging
- JSON-formatted logs
- Contextual information
- Log levels and filtering
- Correlation IDs

#### Log Aggregation
- Centralized log collection
- Log parsing and indexing
- Alerting and monitoring
- Audit trail maintenance

### Health Checks

#### Readiness Probes
- Component health verification
- Dependency checking
- Traffic routing control
- Load balancer integration

#### Liveness Probes
- Deadlock detection
- Resource leak identification
- Automatic restart triggers
- Failure recovery

---

For implementation details, see the [Developer Guide](developer-guide.md) and [Testing Guide](testing-guide.md).
