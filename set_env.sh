#!/bin/bash

# This script sets various Google Cloud related environment variables.
# It must be SOURCED to make the variables available in your current shell.
# Example: source ./set_gcp_env.sh

# --- Configuration ---
PROJECT_FILE="$HOME/project_id_v1.txt"
SPANNER_INSTANCE_ID="instavibe-graph-instance-v1"
SPANNER_DATABASE_ID="graphdb_v1"
GOOGLE_CLOUD_LOCATION="us-central1"
REPO_NAME="introveally-repo-v1"
MAPKEY_FILE="$HOME/mapkey_v1.txt"
# ---------------------

echo "--- Setting Google Cloud Environment Variables ---"

# --- Authentication Check ---
echo "Checking gcloud authentication status..."
# Run a command that requires authentication (like listing accounts or printing a token)
# Redirect stdout and stderr to /dev/null so we don't see output unless there's a real error
if gcloud auth print-access-token > /dev/null 2>&1; then
  echo "gcloud is authenticated."
else
  echo "Error: gcloud is not authenticated."
  echo "Please log in by running: gcloud auth login"
  # Use 'return 1' instead of 'exit 1' because the script is meant to be sourced.
  # 'exit 1' would close your current terminal session.
  return 1
fi
# --- --- --- --- --- ---


# 1. Check if project file exists
if [ ! -f "$PROJECT_FILE" ]; then
  echo "Error: Project file not found at '$PROJECT_FILE'"
  echo "Please run the './init.sh' script first to create it."
  return 1 # Return 1 as we are sourcing
fi

# 2. Set the default gcloud project configuration
PROJECT_ID_FROM_FILE=$(cat "$PROJECT_FILE")
echo "Setting gcloud config project to: $PROJECT_ID_FROM_FILE"
# Add error checking. If the project doesn't exist or user lacks permissions, this will fail.
gcloud config set project "$PROJECT_ID_FROM_FILE" --quiet || return 1

# 3. Export PROJECT_ID (Get from config to confirm it was set correctly)
export PROJECT_ID=$(gcloud config get project)
echo "Exported PROJECT_ID=$PROJECT_ID"

# 4. Export PROJECT_NUMBER
# Add error checking and use an intermediate variable for robustness.
PROJECT_NUMBER_VAL=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)") || return 1
export PROJECT_NUMBER=$PROJECT_NUMBER_VAL
echo "Exported PROJECT_NUMBER=$PROJECT_NUMBER"

# 5. Export SERVICE_ACCOUNT_NAME (Default Compute Service Account)
# Add error checking and use an intermediate variable for robustness.
SERVICE_ACCOUNT_NAME_VAL=$(gcloud compute project-info describe --format="value(defaultServiceAccount)") || return 1
export SERVICE_ACCOUNT_NAME=$SERVICE_ACCOUNT_NAME_VAL
echo "Exported SERVICE_ACCOUNT_NAME=$SERVICE_ACCOUNT_NAME"

# 6. Export SPANNER_INSTANCE_ID
# Use the variable defined in the configuration section
export SPANNER_INSTANCE_ID="$SPANNER_INSTANCE_ID"
echo "Exported SPANNER_INSTANCE_ID=$SPANNER_INSTANCE_ID"

# 7. Export SPANNER_DATABASE_ID
# Use the variable defined in the configuration section
export SPANNER_DATABASE_ID="$SPANNER_DATABASE_ID"
echo "Exported SPANNER_DATABASE_ID=$SPANNER_DATABASE_ID"

# 8. Export GOOGLE_CLOUD_PROJECT (Often used by client libraries)
# This is usually the same as PROJECT_ID
export GOOGLE_CLOUD_PROJECT="$PROJECT_ID"
echo "Exported GOOGLE_CLOUD_PROJECT=$GOOGLE_CLOUD_PROJECT"

# 9. Export GOOGLE_GENAI_USE_VERTEXAI
export GOOGLE_GENAI_USE_VERTEXAI="TRUE"
echo "Exported GOOGLE_GENAI_USE_VERTEXAI=$GOOGLE_GENAI_USE_VERTEXAI"

# 10. Export GOOGLE_CLOUD_LOCATION
export GOOGLE_CLOUD_LOCATION="$GOOGLE_CLOUD_LOCATION"
echo "Exported GOOGLE_CLOUD_LOCATION=$GOOGLE_CLOUD_LOCATION"

# 11. Export REPO_NAME
export REPO_NAME="$REPO_NAME"
echo "Exported REPO_NAME=$REPO_NAME"

# 12. Export REGION
export REGION="$GOOGLE_CLOUD_LOCATION"
echo "Exported REGION=$GOOGLE_CLOUD_LOCATION"

# 13. Check for and set GOOGLE_MAPS_API_KEY from mapkey.txt
if [ -f "$MAPKEY_FILE" ]; then
  # File exists
  if [ -s "$MAPKEY_FILE" ]; then
    # File is not empty
    export GOOGLE_MAPS_API_KEY=$(cat "$MAPKEY_FILE")
    echo "Exported GOOGLE_MAPS_API_KEY from $MAPKEY_FILE"
  else
    # File is empty
    echo "WARNING!!WARNING!!WARNING!!WARNING!!WARNING!!WARNING!!WARNING!!WARNING!!"
    echo "------------------GOOGLE_MAPS_API_KEY will not be set.-------------------"
    echo "-------------------------------------------------------------------------"
    echo ""
    echo "Check your key name must be \"Maps Platform API Key\" and rerun 1st section of step 5."
    echo ""
    echo "-------------------------------------------------------------------------"
    echo "WARNING!!WARNING!!WARNING!!WARNING!!WARNING!!WARNING!!WARNING!!WARNING!!"
  fi
fi

echo "--- Environment setup complete ---"
