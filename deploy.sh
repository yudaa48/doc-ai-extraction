#!/bin/bash

# Enhanced Deployment Script for Document AI Extraction Service

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration Variables
PROJECT_ID="neon-camp-449123-j1"
SERVICE_NAME="doc-ai-extraction"
REGION="us-central1"
SERVICE_ACCOUNT="document-ai@neon-camp-449123-j1.iam.gserviceaccount.com"
REPOSITORY="doc-ai-extraction"
IMAGE_NAME="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${SERVICE_NAME}"

# Timestamp for versioning
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
TAGGED_IMAGE="${IMAGE_NAME}:${TIMESTAMP}"

# Error handling function
handle_error() {
    echo -e "${RED}ERROR: $1${NC}"
    exit 1
}

# Prerequisite checks
pre_deploy_checks() {
    echo -e "${YELLOW}Running pre-deployment checks...${NC}"
    
    # Check gcloud is installed
    command -v gcloud >/dev/null 2>&1 || handle_error "gcloud is not installed"
    
    # Check docker is installed
    command -v docker >/dev/null 2>&1 || handle_error "docker is not installed"
    
    # Verify project configuration
    CURRENT_PROJECT=$(gcloud config get-value project)
    if [ "$CURRENT_PROJECT" != "$PROJECT_ID" ]; then
        echo -e "${YELLOW}Switching to project ${PROJECT_ID}${NC}"
        gcloud config set project ${PROJECT_ID}
    fi
}

# Authentication
authenticate() {
    echo -e "${YELLOW}Authenticating with Google Cloud...${NC}"
    gcloud auth configure-docker ${REGION}-docker.pkg.dev || handle_error "Docker authentication failed"
}

# Build Docker image
build_image() {
    echo -e "${YELLOW}Building Docker image...${NC}"
    docker build --platform linux/amd64 -t ${TAGGED_IMAGE} . || handle_error "Docker build failed"
    
    # Optional: Build latest tag as well
    docker tag ${TAGGED_IMAGE} ${IMAGE_NAME}:latest
}

# Push image to Artifact Registry
push_image() {
    echo -e "${YELLOW}Pushing image to Artifact Registry...${NC}"
    docker push ${TAGGED_IMAGE} || handle_error "Image push failed"
    docker push ${IMAGE_NAME}:latest || handle_error "Latest image push failed"
}

# Deploy to Cloud Run
deploy_to_cloud_run() {
    echo -e "${YELLOW}Deploying to Cloud Run...${NC}"
    gcloud run deploy ${SERVICE_NAME} \
        --image ${TAGGED_IMAGE} \
        --platform managed \
        --region ${REGION} \
        --service-account ${SERVICE_ACCOUNT} \
        --allow-unauthenticated \
        --memory 4Gi \
        --cpu 2 \
        --timeout 900 \
        --max-instances 10 \
        --port 8080 \
        --set-env-vars=ENV=production \
        || handle_error "Cloud Run deployment failed"
}

# Verify deployment
verify_deployment() {
    echo -e "${YELLOW}Verifying deployment...${NC}"
    DEPLOYMENT_URL=$(gcloud run services describe ${SERVICE_NAME} --region=${REGION} --format='value(status.url)')
    
    # Perform health check
    HEALTH_CHECK=$(curl -s ${DEPLOYMENT_URL}/health || echo "Health check failed")
    
    if [[ "$HEALTH_CHECK" == *"OK"* ]]; then
        echo -e "${GREEN}✔ Deployment successful!${NC}"
        echo -e "${GREEN}Deployment URL: ${DEPLOYMENT_URL}${NC}"
    else
        echo -e "${RED}❌ Health check failed${NC}"
    fi
}

# Cleanup old images (optional)
cleanup_old_images() {
    echo -e "${YELLOW}Cleaning up old images...${NC}"
    # Remove docker images older than 30 days from local and registry
    docker image prune -f --filter "until=720h"
}

# Main deployment workflow
main() {
    pre_deploy_checks
    authenticate
    build_image
    push_image
    deploy_to_cloud_run
    verify_deployment
    cleanup_old_images
}

# Run the deployment
main

echo -e "${GREEN}Deployment completed successfully!${NC}"