#!/bin/bash

# Set variables
PROJECT_ID="neon-camp-449123-j1"
SERVICE_NAME="doc-ai-extraction"
REGION="us-central1"
SERVICE_ACCOUNT="document-ai@neon-camp-449123-j1.iam.gserviceaccount.com"
REPOSITORY="doc-ai-extraction"
IMAGE_NAME="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${SERVICE_NAME}"

# Authenticate and set project
gcloud auth configure-docker ${REGION}-docker.pkg.dev
gcloud config set project ${PROJECT_ID}

# Build the Docker image with platform specification
docker build --platform linux/amd64 -t ${IMAGE_NAME} .

# Push the image to Artifact Registry
docker push ${IMAGE_NAME}

# Deploy to Cloud Run
gcloud run deploy ${SERVICE_NAME} \
    --image ${IMAGE_NAME} \
    --platform managed \
    --region ${REGION} \
    --service-account ${SERVICE_ACCOUNT} \
    --allow-unauthenticated \
    --memory 4Gi \
    --cpu 2 \
    --timeout 900 \
    --max-instances 10 \
    --port 8080