# Neo4j Enterprise Operator for Kubernetes

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Report Card](https://goreportcard.com/badge/github.com/neo4j-labs/neo4j-kubernetes-operator)](https://goreportcard.com/report/github.com/neo4j-labs/neo4j-kubernetes-operator)
[![GitHub Release](https://img.shields.io/github/release/neo4j-labs/neo4j-kubernetes-operator.svg)](https://github.com/neo4j-labs/neo4j-kubernetes-operator/releases)
[![Enterprise Only](https://img.shields.io/badge/Neo4j-Enterprise%20Only-red.svg)](https://neo4j.com/enterprise)
[![Min Version](https://img.shields.io/badge/Neo4j-5.26%2B-blue.svg)](https://neo4j.com/docs)

> 🏢 **ENTERPRISE EDITION ONLY**: This operator exclusively supports Neo4j Enterprise Edition 5.26 and above. Community Edition is NOT supported.

The Neo4j Enterprise Operator for Kubernetes provides a complete solution for deploying, managing, and scaling Neo4j Enterprise clusters in Kubernetes environments. Built with cloud-native best practices, it offers enterprise-grade features including high availability, automated backups, security, and comprehensive observability.

## 🚀 Quick Start

**New to Neo4j or Kubernetes?** → [📖 Quickstart Guide](docs/quickstart.md)

**Ready for production?** → [📋 Complete Documentation](docs/README.md)

### 5-Minute Demo

```bash
# 1. Install the operator
kubectl apply -f https://github.com/neo4j-labs/neo4j-kubernetes-operator/releases/latest/download/neo4j-operator.yaml

# 2. Create authentication secret
kubectl create secret generic neo4j-auth \
  --from-literal=username=neo4j \
  --from-literal=password=mySecurePassword123

# 3. Deploy a Neo4j cluster
cat <<EOF | kubectl apply -f -
apiVersion: neo4j.neo4j.com/v1alpha1
kind: Neo4jEnterpriseCluster
metadata:
  name: my-neo4j-cluster
spec:
  image:
    repo: neo4j
    tag: "5.26-enterprise"
  topology:
    primaries: 3
    secondaries: 1
  storage:
    className: "standard"
    size: "10Gi"
  auth:
    provider: native
    secretRef: neo4j-auth
EOF

# 4. Access your database
kubectl port-forward service/my-neo4j-cluster-client 7474:7474 7687:7687
# Open http://localhost:7474 in your browser
```

## ✨ Key Features

### 🏗️ **Enterprise-Grade Architecture**

- **High Availability**: Multi-replica clusters with automatic failover
- **Topology-Aware Placement**: Distribute instances across availability zones
- **Intelligent Auto-scaling**: Dynamic scaling based on CPU, memory, connections, and query performance
- **Multi-cluster Deployments**: Cross-region and multi-cloud support with service mesh integration

### 🔒 **Security & Compliance**

- **Enterprise Authentication**: LDAP, Active Directory, JWT, and Kerberos integration
- **TLS Encryption**: End-to-end encryption with cert-manager and external secrets integration
- **RBAC Integration**: Kubernetes-native role-based access control
- **OpenShift Certified**: Red Hat certified for enterprise platforms

### 📊 **Data Protection & Recovery**

- **Automated Backups**: Scheduled backups to cloud storage (S3, GCS, Azure Blob)
- **Point-in-Time Recovery**: Restore to specific timestamps with comprehensive validation
- **Disaster Recovery**: Cross-region replication and automated failover coordination
- **Multi-Database Support**: Isolated databases within clusters with granular permissions

### 🔧 **Operations & Monitoring**

- **Prometheus Integration**: Comprehensive metrics collection and alerting
- **Query Performance Monitoring**: Real-time query analysis, slow query detection, and optimization recommendations
- **Plugin Management**: Dynamic plugin installation with version management
- **Rolling Updates**: Zero-downtime upgrades with comprehensive health checks

### ⚡ **Performance & Scaling**

- **Auto-scaling Engine**: Multi-metric scaling with CPU, memory, connection count, query latency, and custom metrics
- **Zone-Aware Scaling**: Intelligent distribution across availability zones
- **Quorum Protection**: Maintains cluster integrity during scaling operations
- **Custom Scaling Algorithms**: Webhook-based and machine learning-powered scaling decisions

## 📋 Documentation

| Guide | Description | Audience |
|-------|-------------|----------|
| [📖 Quickstart](docs/quickstart.md) | Get started in 5 minutes | New users |
| [📋 Complete Documentation](docs/README.md) | All guides and references | All users |
| [🔧 Auto-scaling Guide](docs/auto-scaling-guide.md) | Intelligent scaling configuration | Production users |
| [🌍 Multi-cluster Guide](docs/multi-cluster-deployment-guide.md) | Global deployments | Enterprise users |

### 🎯 **Quick Navigation**

- **New to Neo4j?** → [Quickstart Guide](docs/quickstart.md)
- **Production deployment?** → [Multi-cluster Guide](docs/multi-cluster-deployment-guide.md)
- **Need high availability?** → [Topology-Aware Placement](docs/topology-aware-placement.md)
- **Planning disaster recovery?** → [Disaster Recovery Guide](docs/disaster-recovery-guide.md)
- **Performance optimization?** → [Performance Guide](docs/performance-guide.md)
- **Auto-scaling setup?** → [Auto-scaling Guide](docs/auto-scaling-guide.md)

## 🏢 Enterprise & OpenShift

### Red Hat OpenShift Certification

- ✅ **Certified for OpenShift** 4.10+ with restricted-v2 SCC
- ✅ **UBI-based images** for enterprise compliance
- ✅ **OLM integration** via OperatorHub
- ✅ **Multi-architecture support** (amd64, arm64)

**OpenShift Deployment:** [OpenShift Certification Guide](docs/openshift-certification.md)

### Enterprise Support

- **Professional Services**: Architecture, implementation, and optimization
- **24/7 Support**: Enterprise SLA with dedicated customer success
- **Training & Certification**: GraphAcademy courses and custom training

**Contact**: [Neo4j Enterprise Sales](https://neo4j.com/contact-us/)

## 🤝 Community & Support

### Getting Help

- **📚 Documentation**: [Complete guides](docs/README.md)
- **💬 Community**: [Neo4j Community Forum](https://community.neo4j.com/)
- **🐛 Issues**: [GitHub Issues](https://github.com/neo4j-labs/neo4j-kubernetes-operator/issues)
- **🏢 Enterprise**: [Neo4j Support Portal](https://support.neo4j.com/)

### Contributing

We welcome contributions! This project follows standard open-source contribution practices:

- **Bug Reports**: Use GitHub Issues with detailed reproduction steps
- **Feature Requests**: Discuss in GitHub Discussions before implementation
- **Code Contributions**: Fork, create feature branch, and submit pull request
- **Documentation**: Help improve user guides and API documentation

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

---

**Ready to get started?** → [📖 Quickstart Guide](docs/quickstart.md)

**Questions?** → [💬 Community Forum](https://community.neo4j.com/) | [📧 Enterprise Support](https://support.neo4j.com/)
