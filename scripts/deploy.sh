#!/bin/bash

# Video Transcoding Service Deployment Script
# Usage: ./scripts/deploy.sh

set -e  # Exit on any error

# Configuration
SERVER="wtwh1"
REMOTE_DIR="/var/www/transcoder"
PM2_APP_NAME="transcoder"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[DEPLOY]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in the project root
if [ ! -f "package.json" ]; then
    print_error "deploy.sh must be run from the project root directory"
    exit 1
fi

# Check if .env.production exists
if [ ! -f ".env.production" ]; then
    print_error ".env.production file not found"
    print_error "Please create .env.production with your production environment variables"
    exit 1
fi

# Check if scripts directory exists
if [ ! -d "scripts" ]; then
    mkdir -p scripts
    print_warning "Created scripts directory"
fi

print_status "Starting deployment to $SERVER..."

# Test SSH connection
print_status "Testing SSH connection to $SERVER..."
if ! ssh -o ConnectTimeout=10 "$SERVER" "echo 'SSH connection successful'" >/dev/null 2>&1; then
    print_error "Cannot connect to $SERVER via SSH"
    print_error "Make sure your SSH key is set up and the server is accessible"
    exit 1
fi
print_success "SSH connection verified"

# Create remote directory if it doesn't exist
print_status "Ensuring remote directory exists..."
ssh "$SERVER" "mkdir -p $REMOTE_DIR"

# Sync files using rsync
print_status "Syncing files to server..."
rsync -avz --progress \
    --exclude='.git' \
    --exclude='node_modules' \
    --exclude='uploads/*' \
    --exclude='database.sqlite' \
    --exclude='database.sqlite-wal' \
    --exclude='database.sqlite-shm' \
    --exclude='.env' \
    --exclude='.env.production' \
    --exclude='*.log' \
    --exclude='.DS_Store' \
    --exclude='Thumbs.db' \
    --delete \
    ./ "$SERVER:$REMOTE_DIR/"

if [ $? -eq 0 ]; then
    print_success "Files synced successfully"
else
    print_error "File sync failed"
    exit 1
fi

# Copy .env.production as .env to server
print_status "Copying production environment file..."
scp .env.production "$SERVER:$REMOTE_DIR/.env"

if [ $? -eq 0 ]; then
    print_success "Production environment file copied"
else
    print_error "Failed to copy production environment file"
    exit 1
fi

# Run deployment commands on the server
print_status "Running deployment commands on server..."

ssh "$SERVER" << EOF
    set -e
    cd $REMOTE_DIR
    
    echo "[REMOTE] Installing/updating Node.js dependencies..."
    npm install --production
    
    echo "[REMOTE] Checking PM2 status..."
    if pm2 describe $PM2_APP_NAME > /dev/null 2>&1; then
        echo "[REMOTE] Restarting PM2 application: $PM2_APP_NAME"
        pm2 restart $PM2_APP_NAME
    else
        echo "[REMOTE] Starting new PM2 application: $PM2_APP_NAME"
        pm2 start src/app.js --name $PM2_APP_NAME
    fi
    
    echo "[REMOTE] Saving PM2 configuration..."
    pm2 save
    
    echo "[REMOTE] Checking application status..."
    sleep 3
    pm2 show $PM2_APP_NAME
EOF

if [ $? -eq 0 ]; then
    print_success "Remote deployment commands completed successfully"
else
    print_error "Remote deployment commands failed"
    exit 1
fi

# Check if the application is running
print_status "Verifying deployment..."
sleep 5

ssh "$SERVER" << EOF
    echo "[REMOTE] Final status check..."
    pm2 list | grep $PM2_APP_NAME
    echo "[REMOTE] Application logs (last 10 lines):"
    pm2 logs $PM2_APP_NAME --lines 10 --nostream
EOF

print_success "Deployment completed successfully!"
print_status "Application should be running at your server's configured port"
print_status "Use 'ssh $SERVER \"pm2 logs $PM2_APP_NAME\"' to view real-time logs"
