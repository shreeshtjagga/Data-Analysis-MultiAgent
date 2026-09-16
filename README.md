# DataPulse — Multi-Agent AI Data Analysis Platform

A multi-agent AI data intelligence platform that ingests raw tabular datasets (CSV/XLSX) and automatically produces deep statistical analyses, interactive visualizations, conversational natural-language querying, and executive AI insights.

---

## Overview & Problem Solved

**DataPulse** turns raw tabular data (`.csv`, `.xlsx`) into actionable intelligence in seconds using a multi-agent AI pipeline.

- **Eliminates the Analyst Bottleneck:** Replaces hours of manual Python/SQL querying and Excel pivot-tables with instant automated profiling.
- **Replaces Rigid Dashboards:** Allows users to chat directly with their dataset in natural language to answer ad-hoc questions on the fly.
- **Zero-Hallucination Math:** Decouples AI reasoning from mathematical computations — all calculations are executed deterministically on the dataset for 100% numerical accuracy.
- **Privacy-First & Self-Hostable:** Can run completely offline and locally (`USE_LOCAL_DB=true`) so sensitive data never leaves your environment.

---

## Architecture & How It Works

DataPulse orchestrates a modular pipeline of specialized AI agents to process, analyze, and visualize data in seconds:

```
┌─────────────────┐       ┌────────────────────────────────────────────────────────┐       ┌─────────────────┐
│                 │       │                     FastAPI Engine                     │       │                 │
│  User Dataset   │ ────► │  1. Architect Agent   (Schema, profiling & anomalies)  │ ────► │  React / Vite   │
│   (CSV / XLSX)  │       │  2. Statistician      (Distributions, metrics & trends)│       │    Dashboard    │
│                 │       │  3. Visualizer Agent  (Plotly interactive chart engine)│       │  & AI Data Chat │
└─────────────────┘       │  4. Insights Agent    (Groq LLM executive synthesis)   │       └─────────────────┘
                          └────────────────────────────────────────────────────────┘
                                     │                              │
                            ┌─────────────────┐            ┌─────────────────┐
                            │ PostgreSQL / DB │            │   Redis Cache   │
                            └─────────────────┘            └─────────────────┘
```

### Specialized Agents:
- **Architect Agent:** Inspects schema types, validates missing values, evaluates data cleanliness, and infers semantic column roles.
- **Statistician Agent:** Computes central tendencies, skewness, variance, correlations, and dataset distributions.
- **Visualizer Agent:** Recommends optimal chart representations and compiles reactive, interactive Plotly visualization specs.
- **Insights Agent:** Powered by high-throughput Groq LLMs to produce strategic summaries, actionable opportunities, and executive takeaways.
- **Conversational Chat Engine:** Enables natural language querying over active datasets with real-time numeric calculations and query resolution.

---

## Key Features

- **Automated Instant Profiling:** Upload datasets to generate full reports, key KPIs, and distribution metrics automatically.
- **Natural Language Data Chat:** Chat with your data to run ad-hoc calculations, filter rows, aggregate values, and get answers in plain English.
- **Interactive Visualizations:** Zoomable, filterable Plotly charts with auto-selected visual dimensions.
- **Secure Authentication & Sessions:** Supabase Auth with Google OAuth & Email/Password, JWT validation, and session inactivity monitors.
- **Flexible Data Tier:** Works with external managed PostgreSQL (Supabase / AWS RDS / Neon) or isolated local fallback mode.
- **High-Performance Caching:** Upstash / Redis query caching with graceful in-memory and Parquet storage fallbacks.

---

## Environment Configuration

DataPulse uses environment variables to configure authentication, databases, and AI models.

Create a `.env` file in the **project root** (see [`.env.example`](file:///.env.example)):

```env
# ------------------------------------------------------------------------------
# 1. High-Performance LLM (Groq API)
# ------------------------------------------------------------------------------
GROQ_API_KEY=gsk_your_groq_api_key_here
GROQ_MODEL=qwen/qwen3.8-27b

# ------------------------------------------------------------------------------
# 2. Authentication & Google OAuth (Supabase)
# ------------------------------------------------------------------------------
# Set USE_LOCAL_DB=true for offline local testing without Supabase/Cloud DB
USE_LOCAL_DB=false
SUPABASE_URL=https://your-project-id.supabase.co
SUPABASE_ANON_KEY=your_supabase_anon_key
SUPABASE_SERVICE_ROLE_KEY=your_supabase_service_role_key

# ------------------------------------------------------------------------------
# 3. External Database (PostgreSQL / Supabase / Neon / AWS RDS)
# ------------------------------------------------------------------------------
DATABASE_URL=postgresql://postgres:your_password@your_host:5432/postgres
DB_SSL=require

# ------------------------------------------------------------------------------
# 4. Caching Layer (Redis / Upstash) [Optional]
# ------------------------------------------------------------------------------
REDIS_URL=rediss://default:your_redis_password@your_redis_host:6379

# ------------------------------------------------------------------------------
# 5. App & CORS Settings
# ------------------------------------------------------------------------------
APP_ENV=development
CORS_ORIGINS=http://localhost:5173,http://localhost:3000
MAX_UPLOAD_BYTES=26214400
```

### Frontend Configuration
Create `frontend/.env` (see [`frontend/.env.example`](file:///frontend/.env.example)):
```env
VITE_SUPABASE_URL=https://your-project-id.supabase.co
VITE_SUPABASE_ANON_KEY=your_supabase_anon_key
VITE_API_BASE=/api
```

> **Local / Offline Setup:**  
> To run completely offline or on a local network without cloud services, set `USE_LOCAL_DB=true` in `.env`. The backend will automatically use local SQLite storage and mock authentication.

---

## Quickstart (Running Without Scripts)

### 1. Prerequisites
- **Python:** 3.11+ ([python.org](https://www.python.org/downloads/))
- **Node.js:** 18+ ([nodejs.org](https://nodejs.org/))

---

### 2. Backend Setup
Open a terminal in the project root:

```bash
# 1. Navigate to backend
cd backend

# 2. Create and activate a Python virtual environment
python -m venv .venv

# On Windows:
.venv\Scripts\activate
# On macOS / Linux:
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Launch FastAPI server
uvicorn api:app --reload --host 0.0.0.0 --port 8000
```
> Backend API will be available at: **http://localhost:8000** (Interactive Docs: `http://localhost:8000/docs`)

---

### 3. Frontend Setup
Open a second terminal in the project root:

```bash
# 1. Navigate to frontend
cd frontend

# 2. Install Node dependencies
npm install

# 3. Start development server
npm run dev
```
> Web Dashboard will be available at: **http://localhost:5173**

---

## Repository Structure

```
Data-Analysis-MultiAgent/
├── backend/
│   ├── agents/               # Multi-agent analysis pipeline
│   │   ├── architect.py      # Schema inference & dataset profiling
│   │   ├── statistician.py   # Statistical & numerical computations
│   │   ├── visualizer.py     # Plotly visualization generator
│   │   ├── plot_generator.py # Chart engine utilities
│   │   └── insights.py       # Groq LLM executive synthesis
│   ├── core/                 # Shared chat engine, LLM client, cache & utils
│   │   ├── chat_engine.py    # Natural language data query engine
│   │   ├── llm_client.py     # Groq client & retry handler
│   │   ├── cache.py          # Redis / memory cache manager
│   │   └── constants.py      # System prompts & configurations
│   ├── models/               # Pydantic schemas & state models
│   ├── storage/              # Parquet caching & persistent file helpers
│   ├── api.py                # FastAPI route controllers & middleware
│   ├── auth.py               # Supabase JWT & OAuth verification
│   ├── db.py                 # SQLAlchemy async PostgreSQL engine & models
│   ├── analysis_history.py   # Analysis record management
│   ├── requirements.txt      # Backend Python dependencies
│   └── Dockerfile            # Container deployment configuration
│
├── frontend/
│   ├── src/
│   │   ├── components/       # UI components, visualizers & error boundaries
│   │   ├── pages/            # Dashboard & Auth views
│   │   ├── api.js            # Axios client, Supabase auth & API helpers
│   │   ├── App.jsx           # Application routing & state
│   │   ├── main.jsx          # React DOM entrypoint
│   │   └── index.css         # Global design system & theme
│   ├── index.html            # Single page application HTML template
│   ├── package.json          # Frontend dependencies & scripts
│   ├── vite.config.js        # Vite bundler & proxy configuration
│   └── .env.example          # Frontend environment template
│
├── .env.example              # Root environment variables template
├── .gitignore                # Git ignore rules (secrets, venvs, caches)
├── sample_sales_data.csv     # Sample dataset for testing
└── README.md                 # Project documentation
```
