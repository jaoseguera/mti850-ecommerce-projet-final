# MTI850 - Final Project: E-Commerce Customer Analytics & Repeat Purchase Prediction

## Use Case 3: E-Commerce Customer Analytics and Recommendation

Projet final dans le cadre du cours **MTI850 - Analytiques des données massives** (Maîtrise en technologies de l'information, ÉTS Montréal).

---

## 1. Description du Projet

Ce projet implémente un pipeline d'analyse de données massives distribué de bout en bout avec **Apache Spark (PySpark)**, **HDFS**, **Spark MLlib** et **Spark Structured Streaming**, appliqué au jeu de données public **Olist Brazilian E-Commerce**.

### Problématique d'affaires
Dans le secteur du commerce électronique, le coût d'acquisition de nouveaux clients est considérablement plus élevé que la fidélisation des clients existants. L'objectif est de prédire, à partir du comportement initial d'un client et de son historique de commande, s'il effectuera un **réachat** (*Repeat Purchase / Customer Retention*) dans une fenêtre d'observation future.

### Défi Big Data
- **Modèle de données relationnel multi-tables :** Intégration distribuée de 9 tables (commandes, articles, clients, produits, paiements, avis, vendeurs, géolocalisation).
- **Prévention stricte du *Data Leakage* :** Découpage temporel (*time-based train/test split*) respectant l'ordre chronologique des transactions plutôt qu'un split aléatoire.
- **Inférence en continu :** Déploiement du modèle entraîné hors-ligne sur un flux d'événements entrants (*Spark Structured Streaming*) avec calcul incrémental et fenêtrage temporel (*event-time windowing*).

---

## 2. Architecture de la Solution

```mermaid
flowchart TD
    subgraph S1["1. Ingestion & Stockage Distribué"]
        A["Fichiers CSV Olist (9 tables)"] --> B["HDFS Data Lake (/user/mti850/raw)"]
    end

    subgraph S2["2. Traitement PySpark & Préparation"]
        B --> C["Nettoyage & Casts temporels"]
        C --> D["Jointures distribuées\n(orders ⋈ items ⋈ customers ⋈ products)"]
        D --> E["Agrégation au niveau client\n(customer_unique_id)"]
    end

    subgraph S3["3. Feature Engineering & Split Anti-Leakage"]
        E --> F["Features RFM & Expérience\n(Récence, Fréquence, Montant, Avis)"]
        F --> G["Découpage Temporel Strict\n(Train: Passé | Test: Futur)"]
        G --> H["Spark ML Pipeline\n(StringIndexer, VectorAssembler, Scaler)"]
    end

    subgraph S4["4. Modélisation Hors-Ligne (MLlib)"]
        H --> I["Baseline heuristique (Règle RFM)"]
        H --> J["Modèles MLlib (GBT / RandomForest)"]
        J --> K["Optimisation (ParamGrid & CrossValidator)"]
        K --> L["Évaluation (ROC-AUC, PR-AUC, F1)"]
        L --> M["Export PipelineModel sérialisé (/user/mti850/models)"]
    end

    subgraph S5["5. Déploiement Streaming Temps Réel"]
        N["Flux de nouvelles commandes\n(HDFS /stream_input)"] --> O["Spark Structured Streaming\n(Micro-batches & Event-Time Window)"]
        M -.->|Chargement du modèle| O
        O --> P["Inférence au fil de l'eau\n(Score de probabilité de réachat)"]
        P --> Q["Actions & Rétention Marketing"]
    end

    style S1 fill:#f0f4f8,stroke:#1e3a8a,stroke-width:2px
    style S2 fill:#f8fafc,stroke:#0284c7,stroke-width:2px
    style S3 fill:#fefce8,stroke:#ca8a04,stroke-width:2px
    style S4 fill:#f0fdf4,stroke:#16a34a,stroke-width:2px
    style S5 fill:#fdf2f8,stroke:#db2777,stroke-width:2px
```

---

## 3. Structure du Répertoire

```text
mti850-ecommerce-analytics-projet-final/
│
├── .gitignore
├── Dockerfile
├── docker-compose.yaml
├── README.md
│
├── data/
│   ├── raw/                          # Données brutes Olist (CSVs Kaggle)
│   ├── processed/                    # Données traitées / Parquet
│   └── stream_input/                 # Fichiers rejoués pour le streaming
│
├── notebooks/
│   └── MTI850_FinalProject_Repeat_Purchase.ipynb  # Notebook principal complet
│
└── src/                              # Scripts utilitaires & pipelines
```

---

## 4. Démarrage Rapide (Environnement Docker Autonome)

Le projet s'exécute dans son propre conteneur dédié intégrant **Apache Hadoop 3.5**, **Apache Spark 4.2**, **Python 3.12** et **JupyterLab**.

### Option A : Avec Docker Compose (Recommandé)

1. **Construire l'image :**
   ```bash
   docker compose build
   ```

2. **Démarrer le conteneur en arrière-plan :**
   ```bash
   docker compose up -d
   ```

3. **Vérifier l'état du conteneur :**
   ```bash
   docker compose ps
   ```

4. **Arrêter le conteneur :**
   ```bash
   docker compose down
   ```

---

### Option B : Avec la CLI Docker (Direct)

1. **Construire l'image depuis le Dockerfile :**
   ```bash
   docker build -t mti850-ecommerce-spark .
   ```

2. **Rouler le conteneur :**
   * Sous PowerShell (Windows) :
     ```powershell
     docker run -d --name mti850-ecommerce-cluster -p 8888:8888 -p 9870:9870 -p 9000:9000 -p 8088:8088 -p 4040-4045:4040-4045 -v ${PWD}:/workspace mti850-ecommerce-spark
     ```
   * Sous Bash / Linux / macOS :
     ```bash
     docker run -d --name mti850-ecommerce-cluster -p 8888:8888 -p 9870:9870 -p 9000:9000 -p 8088:8088 -p 4040-4045:4040-4045 -v $(pwd):/workspace mti850-ecommerce-spark
     ```

---

### Accès aux Services Web
| Service | URL | Note |
|---|---|---|
| **JupyterLab** | http://localhost:8888 | Jeton d'accès (*token*) : `mti850` |
| **HDFS NameNode** | http://localhost:9870 | Gestion du système de fichiers distribué |
| **YARN ResourceManager** | http://localhost:8088 | Gestionnaire de ressources du cluster |
| **Spark Application UI** | http://localhost:4040 | Suivi des jobs Spark en temps réel |

---

## 5. Dataset

Télécharger les données depuis Kaggle :
[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

Placer les fichiers `.csv` décompressés dans le dossier `data/raw/` :
- `olist_customers_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `product_category_name_translation.csv`
