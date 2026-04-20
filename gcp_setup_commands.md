# GCP Infrastructure Setup — Command Reference

## Key Info
```
VM Static IP     : 35.239.47.242
VM Name          : mip-vm
Zone             : us-central1-a
Project          : mip-platform-2024
GCS Buckets      : mip-bronze-2024 / mip-silver-2024 / mip-gold-2024
Airflow UI       : http://35.239.47.242:8080
Workspace (VM)   : /home/bhavyalikhitha_bbl/bhavya-workspace
```

---

## 1. Open Cloud Shell
Go to **console.cloud.google.com** → click `>_` (Cloud Shell) top right.

```bash
# [CLOUD SHELL] Switch to correct project
gcloud config set project mip-platform-2024
```

---

## 2. SSH into VM
```bash
# [CLOUD SHELL]
gcloud compute ssh mip-vm --zone=us-central1-a
```

---

## 3. Create GCP Project (one-time)
```bash
# [CLOUD SHELL]
gcloud projects create mip-platform-2024 --name="MIP Platform"
gcloud config set project mip-platform-2024
gcloud billing projects link mip-platform-2024 --billing-account=01E0CD-414552-4FD7B5
gcloud services enable compute.googleapis.com storage.googleapis.com
```

---

## 4. Create VM + Firewall + GCS Buckets (one-time)
```bash
# [CLOUD SHELL] Create VM (e2-standard-4: 4 vCPU, 16GB RAM, 50GB disk)
gcloud compute instances create mip-vm \
  --zone=us-central1-a \
  --machine-type=e2-standard-4 \
  --boot-disk-size=50GB \
  --boot-disk-type=pd-standard \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --tags=mip-server

# [CLOUD SHELL] Open ports: Airflow(8080), Jupyter(8888), Kafka(9092), Streamlit(8501)
gcloud compute firewall-rules create mip-allow-ports \
  --allow=tcp:8080,tcp:8888,tcp:9092,tcp:9000,tcp:9001,tcp:8501 \
  --target-tags=mip-server \
  --source-ranges=0.0.0.0/0

# [CLOUD SHELL] Open SSH port
gcloud compute firewall-rules create allow-ssh \
  --allow=tcp:22 \
  --target-tags=mip-server \
  --source-ranges=0.0.0.0/0

# [CLOUD SHELL] Assign static IP
gcloud compute addresses create mip-static-ip --region=us-central1
gcloud compute instances delete-access-config mip-vm \
  --zone=us-central1-a --access-config-name="external-nat"
gcloud compute instances add-access-config mip-vm \
  --zone=us-central1-a --access-config-name="external-nat" --address=35.239.47.242

# [CLOUD SHELL] Create GCS buckets
gcloud storage buckets create gs://mip-bronze-2024 --project=mip-platform-2024 --location=us-central1
gcloud storage buckets create gs://mip-silver-2024 --project=mip-platform-2024 --location=us-central1
gcloud storage buckets create gs://mip-gold-2024 --project=mip-platform-2024 --location=us-central1
```

---

## 5. Grant VM Storage Access + HMAC Keys (one-time)
```bash
# [CLOUD SHELL] Grant storage admin to VM service account
gcloud projects add-iam-policy-binding mip-platform-2024 \
  --member="serviceAccount:606310505186-compute@developer.gserviceaccount.com" \
  --role="roles/storage.admin"

# [CLOUD SHELL] Create HMAC keys for boto3 / Kafka Connect
gcloud storage hmac create 606310505186-compute@developer.gserviceaccount.com \
  --project=mip-platform-2024
```
**Credentials (already created — do not regenerate):**
```
GCS_ACCESS_KEY : GOOG1ECMI5556PKW4BG6QK3VL43KFGUZ2XZWA4ZGVF3IVDWK3Q2X6HYAWQ535
GCS_SECRET_KEY : /yluMFMGYXpgcDtKnzszQfRKKyfbFBGpxmcpSQYx
GCS_ENDPOINT   : https://storage.googleapis.com
```

---

## 6. Add Teammates (one-time, run from Cloud Shell)
```bash
# [CLOUD SHELL]
gcloud projects add-iam-policy-binding mip-platform-2024 \
  --member="user:aqeelryan@gmail.com" \
  --role="roles/editor"

gcloud projects add-iam-policy-binding mip-platform-2024 \
  --member="user:vaddadi.d123@gmail.com" \
  --role="roles/editor"

# [CLOUD SHELL] Add teammate SSH keys (replace with their actual public key)
gcloud compute instances add-metadata mip-vm \
  --zone=us-central1-a \
  --metadata="ssh-keys=<their-username>:<their-public-key>"
```

---

## 7. Install Docker on VM (one-time)
```bash
# [VM]
sudo apt update && sudo apt install -y docker.io docker-compose python3-pip
sudo usermod -aG docker $USER
newgrp docker
```

---

## 8. Start Docker Containers (run after every VM restart)
```bash
# [VM]
cd /home/bhavyalikhitha_bbl/bhavya-workspace
docker-compose up -d
docker ps
```
Expected containers: `mip_kafka_1`, `mip_zookeeper_1`, `mip_airflow_1`

---

## 9. Create Kafka Topics (one-time)
```bash
# [VM]
docker exec mip_kafka_1 kafka-topics --create --bootstrap-server localhost:9092 --topic source.off.deltas --partitions 3 --replication-factor 1
docker exec mip_kafka_1 kafka-topics --create --bootstrap-server localhost:9092 --topic source.openfda.recalls --partitions 3 --replication-factor 1
docker exec mip_kafka_1 kafka-topics --create --bootstrap-server localhost:9092 --topic pipeline.events --partitions 1 --replication-factor 1
docker exec mip_kafka_1 kafka-topics --create --bootstrap-server localhost:9092 --topic pipeline.metrics --partitions 1 --replication-factor 1

# [VM] Verify
docker exec mip_kafka_1 kafka-topics --list --bootstrap-server localhost:9092
```

---

## 10. Clone Repo + Setup Workspace (one-time per person)
```bash
# [VM]
cd /home/bhavyalikhitha_bbl
git clone https://github.com/BigDataIA-Spring26-Team-5/Marketplace-Intelligence-Platform.git <your-name>-workspace
cd <your-name>-workspace
git checkout <your-branch>
cp /home/bhavyalikhitha_bbl/bhavya-workspace/.env .env
```

---

## 11. Daily Git Workflow
```bash
# [VM] Start of day
git pull origin <your-branch>

# [VM] End of day
git add .
git commit -m "your message"
git push origin <your-branch>
# Then open PR on GitHub → merge to main
```

---

## 12. VS Code Remote SSH Setup (each teammate, one-time)

**Step 1 — Generate SSH key (local machine PowerShell/terminal):**
```powershell
# [LOCAL]
ssh-keygen -t ed25519 -f "C:\Users\<name>\.ssh\google_compute_engine" -C "<gcp-username>"
cat "C:\Users\<name>\.ssh\google_compute_engine.pub"
```

**Step 2 — Add to SSH config (`C:\Users\<name>\.ssh\config`):**
```
Host mip-vm
  HostName 35.239.47.242
  User <gcp-username>
  IdentityFile C:\Users\<name>\.ssh\google_compute_engine
  IdentitiesOnly yes
```

**Step 3 — Connect in VS Code:**
- Install **Remote - SSH** extension
- `Ctrl+Shift+P` → `Remote-SSH: Connect to Host` → `mip-vm`
- Select **Linux**
- **File → Open Folder** → `/home/bhavyalikhitha_bbl/<your-name>-workspace`

---

## 13. Troubleshooting
```bash
# Docker permission denied
# [VM]
sudo usermod -aG docker $USER && newgrp docker

# Check VM static IP
# [CLOUD SHELL]
gcloud compute instances describe mip-vm --zone=us-central1-a \
  --format="value(networkInterfaces[0].accessConfigs[0].natIP)"

# VM restarted — restart containers
# [VM]
cd /home/bhavyalikhitha_bbl/bhavya-workspace && docker-compose up -d

# Check GCS buckets
# [CLOUD SHELL]
gcloud storage ls gs://mip-bronze-2024
```
