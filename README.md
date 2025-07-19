# Job Search Engine

A powerful Python-based job search engine that provides fast and relevant job search capabilities using Elasticsearch for indexing and search functionality.

## 📋 Prerequisites

Before you begin, ensure you have the following installed on your system:

- **Python 3.13.5**: [Download Python](https://www.python.org/downloads/)
- **pip**: Python package installer (usually comes with Python)
- **Elasticsearch 8.8.0**: [Download Elasticsearch](https://www.elastic.co/downloads/elasticsearch)
- **Git**: [Download Git](https://git-scm.com/downloads)

## 🛠️ Installation & Setup

### Step 1: Clone the Repository

```bash
git clone https://github.com/ali-hassan-dev/search_engine.git
cd search-engine
```

### Step 2: Create Virtual Environment

Create and activate a Python virtual environment to isolate project dependencies:

#### On Windows:
```bash
# Create virtual environment
python -m venv job_search_env

# Activate virtual environment
job_search_env\Scripts\activate
```

#### On macOS/Linux:
```bash
# Create virtual environment
python3 -m venv job_search_env

# Activate virtual environment
source job_search_env/bin/activate
```

**Note**: You should see `(job_search_env)` in your terminal prompt, indicating the virtual environment is active.

### Step 3: Install Dependencies

With the virtual environment activated, install the required packages:

```bash
pip install -r requirements.txt
```

### Step 4: Configure Environment Variables

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Edit the `.env` file with your actual configuration:
```bash
# MySQL Database Configuration
DB_HOST=your_database_host
DB_PORT=your_database_port
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password

# Elasticsearch Configuration
ES_HOST=localhost
ES_PORT=9200

# API Configuration
MAX_RECORDS=50000
```

### Step 5: Start Elasticsearch

Make sure Elasticsearch is running on your system:

#### Using Docker (Recommended):
```bash
docker run -d \
  --name elasticsearch \
  -p 9200:9200 \
  -p 9300:9300 \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=false" \
  elasticsearch:8.8.0
```

#### Using Local Installation:
```bash
# Start Elasticsearch service
# On macOS with Homebrew:
brew services start elasticsearch

# On Ubuntu/Debian:
sudo systemctl start elasticsearch

# On Windows:
# Navigate to Elasticsearch installation directory and run:
bin\elasticsearch.bat
```

Verify Elasticsearch is running by visiting: http://localhost:9200

## 🚀 Running the Application

### Start the Application

With your virtual environment activated and Elasticsearch running:

```bash
python search_engine.py
```

The application will start on `http://localhost:5000`

### Verify Installation

1. Open your web browser and navigate to: http://localhost:5000
2. You should see the job search interface
3. Try some test searches to ensure everything is working

## 🔍 Usage Examples

### Web Interface

Visit `http://localhost:5000` and use the search interface to find jobs:

- Search for **"développeur"** (French for developer)
- Search for **"drone"** or **"drones"** for drone-related positions
- Search for **"no-code"** for no-code platform jobs
- Search for **"Python"**, **"JavaScript"**, **"React"**, etc.

## 🐛 Troubleshooting

### Common Issues

**1. Elasticsearch not responding**
```
ConnectionError: Connection to Elasticsearch failed
```
- Verify Elasticsearch is running: `curl http://localhost:9200`
- Check Elasticsearch logs for errors
- Ensure correct host and port in `.env` file

**2. Module not found errors**
```
ModuleNotFoundError: No module named 'flask'
```
- Ensure virtual environment is activated
- Reinstall requirements: `pip install -r requirements.txt`

**3. Database connection errors**
```
Database connection failed
```
- Verify database credentials in `.env` file
- Ensure database server is running
- Check database connectivity

**4. Port already in use**
```
OSError: [Errno 48] Address already in use
```
- Kill existing processes on port 5000: `lsof -ti:5000 | xargs kill -9`
- Or change the port in your configuration

## 🔄 Deactivating Virtual Environment

When you're done working on the project, deactivate the virtual environment:

```bash
deactivate
```

---

**Happy job searching! 🎉**
