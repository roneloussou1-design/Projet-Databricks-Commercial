🟦 1. Titre + contexte
# 🚀 DevOps sur Azure Databricks

## 🎯 Objectif du projet
Mettre en place un pipeline data automatisé avec :
- Versioning (GitHub)
- CI/CD (GitHub Actions)
- Infrastructure as Code (Databricks Asset Bundle)

🟦 2. Architecture du projet
## 🏗️ Architecture

Pipeline en 3 couches :
- Bronze → ingestion
- Silver → transformation
- Gold → data business

+ Tests de qualité + CI/CD

🟦 3. Stack technique
## 🛠️ Stack utilisée

- Azure Databricks
- Python / PySpark
- GitHub Actions
- pytest
- Databricks CLI

🟦 4. DevOps mis en place 
## ⚙️ Mise en place DevOps

### 1. Versioning
Tous les notebooks sont versionnés sur GitHub

### 2. CI/CD
Pipeline GitHub Actions :
- Tests automatiques (pytest)
- Déploiement automatique

### 3. Infrastructure as Code
- databricks.yml
- Déploiement automatisé des jobs

🟦 5. Pipeline CI/CD
## 🔄 CI/CD

Déclenchement :
- push sur develop → déploiement DEV
- push sur master → déploiement PROD

Étapes :
1. Lancement des tests
2. Si OK → déploiement Databricks

🟦 6. Structure du repo
## 📁 Structure
Projet/
├── src/
├── tests/
├── databricks.yml
├── .github/workflows/ci-cd.yml

🟦 7. Workflow développeur
## 🔁 Workflow

1. Dev → branche develop
2. Tests automatiques
3. Déploiement DEV
4. Merge master
5. Déploiement PROD

🟦 8. Résultats / valeur ajoutée
## ✅ Résultats

- Déploiement automatisé
- Zéro manipulation manuelle
- Fiabilité grâce aux tests
- Séparation DEV / PROD

🟦 9. Améliorations
## 🚀 Améliorations

- Terraform
- Alerting
- MLflow



