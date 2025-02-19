#!/bin/bash

# Couleurs pour les logs
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}=== Initialisation de l'environnement de développement ===${NC}"

# Étape 1 : Vérification des prérequis
echo -e "${GREEN}1. Vérification des prérequis...${NC}"
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker n'est pas installé. Veuillez l'installer avant de continuer.${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Docker Compose n'est pas installé. Veuillez l'installer avant de continuer.${NC}"
    exit 1
fi

if ! command -v mvn &> /dev/null; then
    echo -e "${RED}Maven n'est pas installé. Veuillez l'installer avant de continuer.${NC}"
    exit 1
fi

echo -e "${GREEN}Tous les prérequis sont satisfaits.${NC}"

# Étape 2 : Nettoyage des anciens conteneurs
echo -e "${GREEN}2. Nettoyage des anciens conteneurs et volumes...${NC}"
docker-compose -f docker/compose/dev-compose.yml down -v --remove-orphans

# Étape 3 : Construction du backend Spring Boot (image dev)
echo -e "${GREEN}3. Construction de l'image Docker pour le backend Spring Boot...${NC}"
cd demo || exit 1
docker build -f Dockerfile.dev -t spring-app-dev .
cd ..

# Étape 4 : Lancement de l'environnement avec Docker Compose
echo -e "${GREEN}4. Lancement de l'environnement avec Docker Compose...${NC}"
docker-compose -f docker/compose/dev-compose.yml up --build

# Étape 5 : Vérification des services
echo -e "${GREEN}5. Vérification des services en cours d'exécution...${NC}"
echo "Spring Boot : http://localhost:8080"
echo "Spark UI : http://localhost:4040"
echo "Cassandra : nc -zv localhost 9042"

echo -e "${GREEN}=== Environnement de développement prêt ===${NC}"
