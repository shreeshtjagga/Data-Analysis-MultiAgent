# DataPulse - Multi-Agent Data Analysis System

DataPulse is a robust, full-stack application that leverages a multi-agent AI architecture to automatically analyze, clean, and visualize user-uploaded datasets.

---

## 📁 Project Structure

```text
DataPulse/
│
├── backend/                  # FastAPI Application
│   ├── agents/               # Multi-agent pipeline logic
│   │   ├── architect.py      # Cleans, profiles, and detects types
│   │   ├── statistician.py   # Computes distribution, skew, and stats
│   │   ├── visualizer.py     # Generates Plotly charts
│   │   └── insights.py       # AI-driven semantic insights
│   ├── core/                 # App logic, cache, graph state
│   ├── models/               # Pydantic data schemas
│   ├── api.py                # Core application routes
│   ├── auth.py               # JWT authentication
│   ├── db.py                 # PostgreSQL connection logic
│   └── analysis_history.py   # DB storage and retention policies
│
├── frontend/                 # React + Vite Application
│   ├── src/                  # React components & UI logic
│   │   ├── datapulse_dashboard.jsx # Main user interface
│   │   └── ...
│   └── vite.config.js        # Vite build configuration
│
├── docker-compose.yml        # Docker composition for all services
├── requirements.txt          # Python dependencies
├── start-backend.bat         # Windows quick-start script (Backend)
├── start-frontend.bat        # Windows quick-start script (Frontend)
├── .env.example              # Example environment variables
└── README.md
```

---

## 🔄 System Flow

The backend utilizes an intelligent node-based graph approach. When a file is uploaded (or fetched from cache), it goes through the following isolated agents sequentially:

1. **Architect Agent:** Reads the dataset, imputes missing data, excludes high-cardinality/unnecessary IDs, detects data types, and uses an LLM to profile the business domain of the dataset.
2. **Statistician Agent:** Scans the clean data to compute mathematical summaries, detect massive outliers, perform multivariate correlation tests, and map distributions.
3. **Visualizer Agent:** Scores reliable combinations of chart structures based on completeness/cardinality, emitting the 6 best diverse Plotly charts (e.g., histograms, correlation heatmaps, line charts).
4. **Insights Agent:** Reads the slimmed output from the Statistician and asks a secure LLM instance to generate 5-8 actionable, human-readable insights and observations.

*Outputs are safely stored in PostgreSQL and temporarily cached via Redis (3-day retention logic) to drastically improve concurrent loading speeds.*

---

## 🚀 How to Run (Using Docker)

The easiest and most isolated way to run DataPulse is via Docker. The `docker-compose.yml` file seamlessly spins up the PostgreSQL database, Redis caching server, FastAPI backend, and Vite frontend.

### Prerequisites
- Install [Docker Desktop](https://www.docker.com/products/docker-desktop/)

### 1. Configure the Environment
Ensure your `.env` file is present in the root folder with the following variables:
```env
# Required for Insights Agent
GROQ_API_KEY=your_groq_api_key

# Database and Cache Networking (use the docker service names)
DATABASE_URL=postgresql+asyncpg://postgres:postgres@db:5432/datapulse
REDIS_URL=redis://redis:6379

# Google Authentication
VITE_GOOGLE_CLIENT_ID=your_client_id
```

### 2. Start the Containers
Open your terminal in the root directory and run:
```bash
docker-compose up --build -d
```

### 3. Access the Application
- **Frontend Dashboard:** [http://localhost:5173](http://localhost:5173)
- **Backend API Docs:** [http://localhost:8000/docs](http://localhost:8000/docs)

*To completely stop the environment and shut down the containers, run:*
```bash
docker-compose down
```

---

## 💻 How to Run (Local Scripts - Windows)

If you prefer to run the application natively without Docker, quick-start `.bat` files are provided:

1. Double-click `start-backend.bat` (This will auto-create your Python `.venv` and install `requirements.txt`).
2. Double-click `start-frontend.bat` (This will auto-check your NPM environments and install `node_modules`).
3. Visit `http://localhost:5173` manually in your browser.

---

## 📜 Development Rules
* Test code locally before initiating a Pull Request.
* Work dynamically by using clear Git commit messages.
* Do not expose secrets or API keys in the `.env` file.
