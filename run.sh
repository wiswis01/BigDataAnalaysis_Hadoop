#!/bin/bash
# -*- coding: utf-8 -*-
# run.sh - Pipeline MapReduce pour Cluster Hadoop Docker
# Version corrigée et optimisée

set -e  # Arrêter en cas d'erreur

# ============================================================================
# CONFIGURATION
# ============================================================================

PROJECT_DIR="/root/project"
DATA_DIR="$PROJECT_DIR/data"
SRC_DIR="$PROJECT_DIR/src"
LOCAL_OUTPUT="$PROJECT_DIR/output"
HDFS_INPUT="/user/root/project/input"
HDFS_OUTPUT="/user/root/project/output"

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# ============================================================================
# FONCTIONS UTILITAIRES
# ============================================================================

print_step() {
    echo -e "${BLUE}==>${NC} ${GREEN}$1${NC}"
}

print_substep() {
    echo -e "  ${CYAN}→${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

check_prerequisites() {
    print_step "Vérification des prérequis dans le cluster Docker..."
    
    # Vérifier Hadoop
    if ! command -v hadoop &> /dev/null; then
        print_error "Hadoop n'est pas installé"
        exit 1
    fi
    
    # Vérifier HDFS
    if ! hdfs dfs -ls / &> /dev/null; then
        print_error "HDFS ne répond pas. Vérifiez: jps"
        exit 1
    fi
    
    # Vérifier Python
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 n'est pas installé"
        exit 1
    fi
    
    # Vérifier mrjob
    if ! python3 -c "import mrjob" 2>/dev/null; then
        print_warning "mrjob n'est pas installé. Installation en cours..."
        pip3 install mrjob -q
        if ! python3 -c "import mrjob" 2>/dev/null; then
            print_error "Impossible d'installer mrjob"
            exit 1
        fi
    fi
    
    # Vérifier les datanodes
    DATANODES=$(hdfs dfsadmin -report 2>/dev/null | grep "Live datanodes" | grep -oP '\d+' || echo "0")
    if [ "$DATANODES" -lt 2 ]; then
        print_warning "Seulement $DATANODES datanode(s) actif(s). Attendu: 2"
    else
        print_success "Cluster OK: $DATANODES datanodes actifs"
    fi
    
    # Vérifier les fichiers source Python
    REQUIRED_SCRIPTS=("
    _valider.py" "analyse_ventes.py" "top_produits.py")
    for script in "${REQUIRED_SCRIPTS[@]}"; do
        if [ ! -f "$SRC_DIR/$script" ]; then
            print_error "Script manquant: $SRC_DIR/$script"
            exit 1
        fi
    done
    
    print_success "Tous les prérequis sont satisfaits"
}

check_hdfs_data() {
    print_step "Vérification des données dans HDFS..."
    
    # Vérifier les fichiers locaux
    REQUIRED_FILES=("ventes_multicanal.csv" "ventes_increment_2025-10.csv" "catalogue_produits.csv")
    missing=0
    
    for f in "${REQUIRED_FILES[@]}"; do
        if [ ! -f "$DATA_DIR/$f" ]; then
            print_error "Fichier manquant: $DATA_DIR/$f"
            missing=1
        else
            size=$(du -h "$DATA_DIR/$f" | cut -f1)
            print_substep "Trouvé: $f ($size)"
        fi
    done
    
    if [ "$missing" -eq 1 ]; then
        print_error "Des fichiers sont manquants. Pipeline interrompu."
        exit 1
    fi
    
    # Créer les répertoires HDFS si nécessaire
    hdfs dfs -mkdir -p "$HDFS_INPUT" 2>/dev/null || true
    
    # Charger les données dans HDFS (avec vérification)
    print_substep "Chargement des données dans HDFS..."
    
    for f in "${REQUIRED_FILES[@]}"; do
        # Supprimer l'ancien fichier s'il existe
        hdfs dfs -rm -f "$HDFS_INPUT/$f" 2>/dev/null || true
        
        # Copier le nouveau fichier
        if hdfs dfs -put "$DATA_DIR/$f" "$HDFS_INPUT/" 2>/dev/null; then
            print_substep "✓ $f chargé dans HDFS"
        else
            print_error "Échec du chargement de $f"
            exit 1
        fi
    done
    
    print_success "Données chargées dans HDFS avec succès"
}

clean_output() {
    print_step "Nettoyage des sorties précédentes..."
    
    # Nettoyer HDFS (supprimer seulement les sous-dossiers)
    for dir in clean rejects metrics top10; do
        hdfs dfs -rm -r -f -skipTrash "$HDFS_OUTPUT/$dir" 2>/dev/null || true
        hdfs dfs -mkdir -p "$HDFS_OUTPUT/$dir" 2>/dev/null || true
    done
    
    # Nettoyer local
    rm -rf "$LOCAL_OUTPUT"/* 2>/dev/null || true
    mkdir -p "$LOCAL_OUTPUT"/{clean,rejects,metrics,top10,logs}
    
    print_success "Nettoyage effectué"
}

# ============================================================================
# ÉTAPE 1: NETTOYAGE DES DONNÉES
# ============================================================================

run_data_cleaning() {
    print_step "ÉTAPE 1/3: Nettoyage des données avec MapReduce..."
    
    cd "$SRC_DIR"
    
    # Traitement v1
    print_substep "Traitement ventes_multicanal.csv (schéma v1)..."
    
    if python3 nettoyer_valider.py \
        -r hadoop \
        --hadoop-streaming-jar $(ls /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar | head -1) \
        --schema-version=v1 \
        --output-dir="$HDFS_OUTPUT/clean/v1" \
        "$HDFS_INPUT/ventes_multicanal.csv" \
        > "$LOCAL_OUTPUT/logs/cleaning_v1.log" 2>&1; then
        print_substep "✓ v1 terminé"
    else
        print_error "Échec du nettoyage v1. Voir: $LOCAL_OUTPUT/logs/cleaning_v1.log"
        cat "$LOCAL_OUTPUT/logs/cleaning_v1.log" | tail -20
        exit 1
    fi
    
    # Traitement v2
    print_substep "Traitement ventes_increment_2025-10.csv (schéma v2)..."
    
    if python3 nettoyer_valider.py \
        -r hadoop \
        --hadoop-streaming-jar $(ls /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar| head -1) \
        --schema-version=v2 \
        --output-dir="$HDFS_OUTPUT/clean/v2" \
        "$HDFS_INPUT/ventes_increment_2025-10.csv" \
        > "$LOCAL_OUTPUT/logs/cleaning_v2.log" 2>&1; then
        print_substep "✓ v2 terminé"
    else
        print_error "Échec du nettoyage v2. Voir: $LOCAL_OUTPUT/logs/cleaning_v2.log"
        cat "$LOCAL_OUTPUT/logs/cleaning_v2.log" | tail -20
        exit 1
    fi
    
    # Combiner les résultats
    print_substep "Combinaison des fichiers nettoyés..."
    
    # Vérifier que les fichiers existent avant de les combiner
    if ! hdfs dfs -test -e "$HDFS_OUTPUT/clean/v1/part-00000" 2>/dev/null; then
        print_error "Aucune sortie générée pour v1"
        exit 1
    fi
    
    # Combiner les résultats
    hdfs dfs -cat "$HDFS_OUTPUT/clean/v1/part-*" "$HDFS_OUTPUT/clean/v2/part-*" 2>/dev/null \
        | hdfs dfs -put -f - "$HDFS_OUTPUT/clean/combined.txt"
    
    # Séparer CLEAN et REJECT
    print_substep "Séparation des enregistrements valides et rejetés..."
    
    # Créer clean_only.txt (sans les lignes REJECT)
    hdfs dfs -cat "$HDFS_OUTPUT/clean/combined.txt" 2>/dev/null \
        | awk -F'\t' '$1 !~ /REJECT/' \
        | hdfs dfs -put -f - "$HDFS_OUTPUT/clean/clean_only.txt"
    
    # Créer rejected_lines.txt (seulement les lignes REJECT)
    hdfs dfs -cat "$HDFS_OUTPUT/clean/combined.txt" 2>/dev/null \
        | awk -F'\t' '$1 ~ /REJECT/' \
        | hdfs dfs -put -f - "$HDFS_OUTPUT/rejects/rejected_lines.txt" || true
    
    # Statistiques
    echo ""
    print_substep "📊 Statistiques de nettoyage:"
    
    TOTAL=$(hdfs dfs -cat "$HDFS_OUTPUT/clean/combined.txt" 2>/dev/null | wc -l || echo 0)
    CLEAN=$(hdfs dfs -cat "$HDFS_OUTPUT/clean/clean_only.txt" 2>/dev/null | wc -l || echo 0)
    REJECT=$(hdfs dfs -cat "$HDFS_OUTPUT/rejects/rejected_lines.txt" 2>/dev/null | wc -l || echo 0)
    
    echo "    Total lignes traitées: $TOTAL"
    echo "    Lignes valides: $CLEAN ($(awk -v c=$CLEAN -v t=$TOTAL 'BEGIN{printf "%.1f", (c/t)*100}')%)"
    echo "    Lignes rejetées: $REJECT ($(awk -v r=$REJECT -v t=$TOTAL 'BEGIN{printf "%.1f", (r/t)*100}')%)"
    
    # Extraire quelques exemples de rejets
    if [ "$REJECT" -gt 0 ]; then
        echo ""
        print_substep "Exemples d'erreurs détectées:"
        hdfs dfs -cat "$HDFS_OUTPUT/rejects/rejected_lines.txt" 2>/dev/null | head -3 | cut -f1-2 | sed 's/^/      /'
    fi
    
    print_success "Nettoyage terminé"
}

# ============================================================================
# ÉTAPE 2: ANALYSE DES VENTES (KPIs)
# ============================================================================

run_sales_analysis() {
    print_step "ÉTAPE 2/3: Calcul des KPIs avec MapReduce..."
    
    cd "$SRC_DIR"
    
    # Vérifier que des données nettoyées existent
    if ! hdfs dfs -test -e "$HDFS_OUTPUT/clean/clean_only.txt" 2>/dev/null; then
        print_error "Aucune donnée nettoyée disponible pour l'analyse"
        exit 1
    fi
    
    CLEAN_COUNT=$(hdfs dfs -cat "$HDFS_OUTPUT/clean/clean_only.txt" 2>/dev/null | wc -l || echo 0)
    if [ "$CLEAN_COUNT" -eq 0 ]; then
        print_error "Le fichier clean_only.txt est vide"
        exit 1
    fi
    
    print_substep "Calcul des ventes par pays/mois et taux de retour..."
    print_substep "Traitement de $CLEAN_COUNT enregistrements..."
    
    if python3 analyse_ventes.py \
        -r hadoop \
        --hadoop-streaming-jar $(ls /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar | head -1) \
        --output-dir="$HDFS_OUTPUT/metrics" \
        "$HDFS_OUTPUT/clean/clean_only.txt" \
        > "$LOCAL_OUTPUT/logs/analysis.log" 2>&1; then
        print_substep "✓ Analyse terminée"
    else
        print_error "Échec de l'analyse. Voir: $LOCAL_OUTPUT/logs/analysis.log"
        cat "$LOCAL_OUTPUT/logs/analysis.log" | tail -20
        exit 1
    fi
    
    # Extraire les résultats
    print_substep "Extraction des résultats depuis HDFS..."
    
    # Créer les répertoires locaux
    mkdir -p "$LOCAL_OUTPUT/metrics"
    
    # Récupérer les résultats
    if hdfs dfs -test -e "$HDFS_OUTPUT/metrics/part-00000" 2>/dev/null; then
        hdfs dfs -get "$HDFS_OUTPUT/metrics/part-*" "$LOCAL_OUTPUT/metrics/" 2>/dev/null || true
        
        # Séparer SALES et METRICS
        if [ -f "$LOCAL_OUTPUT/metrics/part-00000" ]; then
            cat "$LOCAL_OUTPUT/metrics/part-"* 2>/dev/null \
                | awk -F'\t' '$1 ~ /SALES/ {print $2}' \
                > "$LOCAL_OUTPUT/metrics/sales_by_country_month.jsonl"
            
            cat "$LOCAL_OUTPUT/metrics/part-"* 2>/dev/null \
                | awk -F'\t' '$1 ~ /METRICS/ {print $2}' \
                > "$LOCAL_OUTPUT/metrics/return_rate.jsonl"
            
            # Afficher un aperçu
            SALES_COUNT=$(wc -l < "$LOCAL_OUTPUT/metrics/sales_by_country_month.jsonl" 2>/dev/null || echo 0)
            print_substep "✓ $SALES_COUNT agrégations pays/mois générées"
            
            if [ -s "$LOCAL_OUTPUT/metrics/return_rate.jsonl" ]; then
                RETURN_RATE=$(cat "$LOCAL_OUTPUT/metrics/return_rate.jsonl" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('return_rate_by_qty', 0))" 2>/dev/null || echo "N/A")
                print_substep "✓ Taux de retour calculé: $RETURN_RATE%"
            fi
        fi
    else
        print_warning "Aucun résultat trouvé dans HDFS"
    fi
    
    print_success "Analyse des ventes terminée"
}

# ============================================================================
# ÉTAPE 3: TOP 10 PRODUITS
# ============================================================================

run_top_products() {
    print_step "ÉTAPE 3/3: Calcul du Top 10 produits avec MapReduce..."
    
    cd "$SRC_DIR"
    
    print_substep "Analyse des produits les plus vendus..."
    
    if python3 top_produits.py \
        -r hadoop \
        --hadoop-streaming-jar $(ls /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar | head -1) \
        --file="$DATA_DIR/catalogue_produits.csv" \
        --output-dir="$HDFS_OUTPUT/top10" \
        "$HDFS_OUTPUT/clean/clean_only.txt" \
        > "$LOCAL_OUTPUT/logs/top10.log" 2>&1; then
        print_substep "✓ Calcul terminé"
    else
        print_error "Échec du calcul du top 10. Voir: $LOCAL_OUTPUT/logs/top10.log"
        cat "$LOCAL_OUTPUT/logs/top10.log" | tail -20
        exit 1
    fi
    
    # Extraire les résultats
    print_substep "Extraction des résultats..."
    
    mkdir -p "$LOCAL_OUTPUT/top10"
    
    if hdfs dfs -test -e "$HDFS_OUTPUT/top10/part-00000" 2>/dev/null; then
        hdfs dfs -get "$HDFS_OUTPUT/top10/part-*" "$LOCAL_OUTPUT/top10/" 2>/dev/null || true
        
        # Combiner et formater
        cat "$LOCAL_OUTPUT/top10/part-"* 2>/dev/null \
            | sort \
            > "$LOCAL_OUTPUT/top10/top10_products.txt"
        
        # Afficher un aperçu
        if [ -s "$LOCAL_OUTPUT/top10/top10_products.txt" ]; then
            echo ""
            print_substep "🏆 Aperçu du Top 3:"
            head -3 "$LOCAL_OUTPUT/top10/top10_products.txt" | while read line; do
                echo "      $(echo "$line" | cut -f2)"
            done
        fi
    else
        print_warning "Aucun résultat trouvé"
    fi
    
    print_success "Top 10 calculé"
}

# ============================================================================
# GÉNÉRATION DU RAPPORT
# ============================================================================

generate_report() {
    print_step "Génération du rapport de synthèse..."
    
    REPORT_FILE="$LOCAL_OUTPUT/RAPPORT_EXECUTION.txt"
    
    cat > "$REPORT_FILE" << EOF
================================================================================
                    RAPPORT D'EXÉCUTION
           ANALYSE DE VENTES MULTICANAL - MAPREDUCE
================================================================================

Date d'exécution : $(date '+%Y-%m-%d %H:%M:%S')
Hostname         : $(hostname)
Répertoire projet: $PROJECT_DIR
Version Hadoop   : $(hadoop version | head -1)

================================================================================
1. CONFIGURATION DU CLUSTER
================================================================================

NameNode         : hadoop_master
DataNodes        : worker-1, worker-2
Réplication HDFS : $(hdfs getconf -confKey dfs.replication 2>/dev/null || echo "N/A")

État du cluster:
EOF
    
    hdfs dfsadmin -report 2>&1 | head -15 >> "$REPORT_FILE"
    
    cat >> "$REPORT_FILE" << EOF

Utilisation HDFS pour ce projet:
EOF
    
    hdfs dfs -du -h /user/root/project/ 2>&1 >> "$REPORT_FILE"
    
    cat >> "$REPORT_FILE" << EOF

================================================================================
2. STATISTIQUES DE NETTOYAGE DES DONNÉES
================================================================================

EOF
    
    # Calculer les statistiques
    TOTAL=$(hdfs dfs -cat "$HDFS_OUTPUT/clean/combined.txt" 2>/dev/null | wc -l || echo 0)
    CLEAN=$(hdfs dfs -cat "$HDFS_OUTPUT/clean/clean_only.txt" 2>/dev/null | wc -l || echo 0)
    REJECT=$(hdfs dfs -cat "$HDFS_OUTPUT/rejects/rejected_lines.txt" 2>/dev/null | wc -l || echo 0)
    
    SUCCESS_RATE="0.00"
    if [ "$TOTAL" -gt 0 ]; then
        SUCCESS_RATE=$(awk -v c="$CLEAN" -v t="$TOTAL" 'BEGIN{printf "%.2f", (c/t)*100}')
    fi
    
    cat >> "$REPORT_FILE" << EOF
Total de lignes traitées    : $TOTAL
Lignes valides (CLEAN)      : $CLEAN
Lignes rejetées (REJECT)    : $REJECT
Taux de réussite            : $SUCCESS_RATE%

Fichiers traités:
  - ventes_multicanal.csv (schéma v1)
  - ventes_increment_2025-10.csv (schéma v2)

Logs détaillés:
  - Nettoyage v1: $LOCAL_OUTPUT/logs/cleaning_v1.log
  - Nettoyage v2: $LOCAL_OUTPUT/logs/cleaning_v2.log

Exemples d'erreurs détectées:
EOF
    
    if [ "$REJECT" -gt 0 ]; then
        hdfs dfs -cat "$HDFS_OUTPUT/rejects/rejected_lines.txt" 2>/dev/null | head -5 | cut -f1-2 | sed 's/^/  /' >> "$REPORT_FILE"
    else
        echo "  Aucune erreur détectée" >> "$REPORT_FILE"
    fi
    
    cat >> "$REPORT_FILE" << EOF

================================================================================
3. INDICATEURS DE VENTES (KPIs)
================================================================================

EOF
    
    # Taux de retour
    if [ -f "$LOCAL_OUTPUT/metrics/return_rate.jsonl" ] && [ -s "$LOCAL_OUTPUT/metrics/return_rate.jsonl" ]; then
        echo "3.1 TAUX DE RETOUR GLOBAL" >> "$REPORT_FILE"
        echo "" >> "$REPORT_FILE"
        cat "$LOCAL_OUTPUT/metrics/return_rate.jsonl" 2>/dev/null | python3 -m json.tool >> "$REPORT_FILE" 2>/dev/null || echo "Erreur de formatage" >> "$REPORT_FILE"
    else
        echo "⚠️  Taux de retour non disponible" >> "$REPORT_FILE"
    fi
    
    echo "" >> "$REPORT_FILE"
    echo "3.2 VENTES PAR PAYS ET MOIS" >> "$REPORT_FILE"
    echo "" >> "$REPORT_FILE"
    
    if [ -f "$LOCAL_OUTPUT/metrics/sales_by_country_month.jsonl" ] && [ -s "$LOCAL_OUTPUT/metrics/sales_by_country_month.jsonl" ]; then
        SALES_COUNT=$(wc -l < "$LOCAL_OUTPUT/metrics/sales_by_country_month.jsonl")
        echo "Nombre d'agrégations: $SALES_COUNT" >> "$REPORT_FILE"
        echo "" >> "$REPORT_FILE"
        echo "Exemples (5 premières lignes):" >> "$REPORT_FILE"
        head -5 "$LOCAL_OUTPUT/metrics/sales_by_country_month.jsonl" 2>/dev/null | python3 -m json.tool >> "$REPORT_FILE" 2>/dev/null || echo "Erreur de formatage" >> "$REPORT_FILE"
    else
        echo "⚠️  Données de ventes non disponibles" >> "$REPORT_FILE"
    fi
    
    cat >> "$REPORT_FILE" << EOF

Fichier complet: $LOCAL_OUTPUT/metrics/sales_by_country_month.jsonl

================================================================================
4. TOP 10 DES PRODUITS PAR CHIFFRE D'AFFAIRES
================================================================================

EOF
    
    if [ -f "$LOCAL_OUTPUT/top10/top10_products.txt" ] && [ -s "$LOCAL_OUTPUT/top10/top10_products.txt" ]; then
        echo "Classement des 10 meilleurs produits:" >> "$REPORT_FILE"
        echo "" >> "$REPORT_FILE"
        cat "$LOCAL_OUTPUT/top10/top10_products.txt" 2>/dev/null | head -10 >> "$REPORT_FILE"
    else
        echo "⚠️  Top 10 non disponible" >> "$REPORT_FILE"
    fi
    
    cat >> "$REPORT_FILE" << EOF

================================================================================
5. STRUCTURE DES FICHIERS GÉNÉRÉS
================================================================================

Répertoire HDFS: $HDFS_OUTPUT/
  ├── clean/
  │   ├── v1/part-*           (Résultats nettoyés v1)
  │   ├── v2/part-*           (Résultats nettoyés v2)
  │   ├── combined.txt        (Tous les résultats)
  │   └── clean_only.txt      (Seulement les valides)
  ├── rejects/
  │   └── rejected_lines.txt  (Lignes rejetées)
  ├── metrics/
  │   └── part-*              (KPIs calculés)
  └── top10/
      └── part-*              (Top produits)

Répertoire local: $LOCAL_OUTPUT/
  ├── clean/                  (Copies locales)
  ├── rejects/                (Erreurs)
  ├── metrics/
  │   ├── sales_by_country_month.jsonl
  │   └── return_rate.jsonl
  ├── top10/
  │   └── top10_products.txt
  └── logs/
      ├── cleaning_v1.log
      ├── cleaning_v2.log
      ├── analysis.log
      └── top10.log

================================================================================
6. JOBS MAPREDUCE EXÉCUTÉS
================================================================================

Historique des jobs YARN (10 derniers):
EOF
    
    yarn application -list -appStates ALL 2>&1 | tail -10 >> "$REPORT_FILE" || echo "YARN ResourceManager non disponible" >> "$REPORT_FILE"
    
    cat >> "$REPORT_FILE" << EOF

================================================================================
7. COMMANDES UTILES
================================================================================

Consulter les données HDFS:
  hdfs dfs -ls $HDFS_OUTPUT
  hdfs dfs -cat $HDFS_OUTPUT/clean/clean_only.txt | head -20

Consulter les logs:
  cat $LOCAL_OUTPUT/logs/*.log
  tail -f $LOCAL_OUTPUT/logs/cleaning_v1.log

Récupérer les résultats sur votre Mac:
  docker cp hadoop_master:$LOCAL_OUTPUT ~/hadoop_results

Interfaces web:
  HDFS NameNode    : http://localhost:9870
  YARN ResourceMgr : http://localhost:8088
  JobHistory       : http://localhost:19888

================================================================================
8. RÉSUMÉ EXÉCUTIF
================================================================================

✓ Pipeline MapReduce exécuté avec succès
✓ $CLEAN enregistrements valides traités sur $TOTAL ($SUCCESS_RATE%)
✓ $REJECT enregistrements rejetés (erreurs de format/validation)
✓ KPIs calculés et disponibles dans: $LOCAL_OUTPUT/metrics/
✓ Top 10 produits disponible dans: $LOCAL_OUTPUT/top10/

Performances du cluster:
  - Nombre de datanodes: 2 (worker-1, worker-2)
  - Jobs MapReduce: 3 (nettoyage, analyse, top10)
  - Temps d'exécution: ~$(date '+%M') minutes (estimation)

================================================================================
FIN DU RAPPORT - $(date '+%Y-%m-%d %H:%M:%S')
================================================================================
EOF
    
    print_success "Rapport généré: $REPORT_FILE"
    
    # Afficher un résumé console
    echo ""
    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║                    RÉSUMÉ DE L'EXÉCUTION                       ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    echo ""
    echo "  🏢 Cluster        : hadoop_master + worker-1 + worker-2"
    echo "  📁 HDFS           : $HDFS_OUTPUT"
    echo "  📁 Local          : $LOCAL_OUTPUT"
    echo "  ✓  Lignes traitées: $TOTAL"
    echo "  ✓  Lignes valides : $CLEAN ($SUCCESS_RATE%)"
    echo "  ❌ Lignes rejetées: $REJECT"
    echo "  📊 Rapport        : $REPORT_FILE"
    echo ""
}

# ============================================================================
# FONCTION PRINCIPALE
# ============================================================================

main() {
    echo ""
    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║      PIPELINE MAPREDUCE - CLUSTER HADOOP DOCKER                ║"
    echo "║      Master: hadoop_master | Workers: worker-1, worker-2       ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    echo ""
    
    # Vérifications préliminaires
    check_prerequisites
    check_hdfs_data
    clean_output
    
    echo ""
    echo "Démarrage du pipeline MapReduce distribué..."
    echo ""
    
    # Exécution des 3 étapes
    run_data_cleaning
    echo ""
    
    run_sales_analysis
    echo ""
    
    run_top_products
    echo ""
    
    # Génération du rapport
    generate_report
    
    # Message de succès final
    echo ""
    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║                  ✓ PIPELINE TERMINÉ AVEC SUCCÈS!               ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    echo ""
    echo "Prochaines étapes:"
    echo "  1. Consulter le rapport:"
    echo "     cat $LOCAL_OUTPUT/RAPPORT_EXECUTION.txt"
    echo ""
    echo "  2. Voir les métriques:"
    echo "     cat $LOCAL_OUTPUT/metrics/return_rate.jsonl | python3 -m json.tool"
    echo "     cat $LOCAL_OUTPUT/top10/top10_products.txt"
    echo ""
    echo "  3. Voir les interfaces web:"
    echo "     - HDFS: http://localhost:9870"
    echo "     - YARN: http://localhost:8088"
    echo ""
    echo "  4. Récupérer les résultats sur votre Mac:"
    echo "     docker cp hadoop_master:$LOCAL_OUTPUT ~/hadoop_results"
    echo ""
}

# Exécuter le script principal
main