# 🚀 Rules Analytics & Automation Pipeline

An automated, end-to-end framework for standardizing, registering, updating, soft-deleting, and archiving property data mapping rules across Staging and Production environments.

---

## 📋 Table of Contents
- [Prerequisites & Sibling Repositories](#-prerequisites--sibling-repositories)
- [Local Machine Setup](#-local-machine-setup)
- [Directory Structure](#-directory-structure)
- [Batch Testing Configuration](#-batch-testing-configuration)
- [Execution Lifecycle (Stage-by-Stage)](#-execution-lifecycle-stage-by-stage)
- [Batch Processing & Shared Rule Safeguards](#-batch-processing--shared-rule-safeguards)
- [Logging Infrastructure](#-logging-infrastructure)
- [Troubleshooting & Common Edge Cases](#-troubleshooting--common-edge-cases)
- [Best Practices & Safety Precautions](#-best-practices--safety-precautions)

---

## 📋 Prerequisites & Sibling Repositories

Before setting up the repository, ensure your environment meets the following software requirements:

* **Python 3.10+** (Python 3.11 recommended)
* **Git** installed and configured
* Active SSH keys and credentials for internal rule microservices and database environments

### Required Sibling Repositories

This project relies on underlying core libraries and bulk update engines. Ensure the following sibling repositories are cloned into the **same parent directory** alongside `rules_analytics`:

```text
parent_folder/
├── rules_analytics/           <-- (This repo)
├── dm-consolidated-rules/     <-- Consolidated rule definitions (ui-rules/ active & archived)
├── eim-mapex/                 <-- Mapex schema mapping dependencies
├── eim-slp-tools/             <-- Execution tools & bulk update engine
├── eim-snowflake-id/          <-- Snowflake ID utilities
├── eim-utilities-pip/         <-- Internal pipeline utility package
└── mapping-slp-rules/         <-- Mapping rules configuration repository
