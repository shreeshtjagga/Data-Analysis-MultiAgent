# DataPulse — AI Data Analysis Platform

Multi-agent AI pipeline that takes any CSV/XLSX dataset and returns charts, statistics, and insights automatically.

---

## Prerequisites

Install these once on your machine before running anything:

| Tool | Version | Download |
|------|---------|----------|
| **Python** | 3.11 or later | https://www.python.org/downloads/ *(check "Add Python to PATH")* |
| **Node.js** | 18 or later | https://nodejs.org/ |

---

## Setup (First Time)

### 1. Clone the repo
```
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
cd YOUR_REPO
```

### 2. Get the `.env` files
Ask the project owner to send you two files securely:
- `.env` → place it in the **project root** (same folder as the `.bat` files)
- `frontend\.env` → place it in the **frontend folder**

### 3. Run the servers

Open **two separate terminal windows** (or double-click the `.bat` files):

**Terminal 1 — Backend:**
```
start-backend.bat
```

**Terminal 2 — Frontend:**
```
start-frontend.bat
```

> The bat files handle everything automatically:
> virtual environment creation, Python dependencies, and Node modules.
> First run takes ~2 minutes to download packages.

### 4. Open the app
Go to → **http://localhost:5173**

---

## Project Structure

```
DD PROJECT/
├── backend/                  # FastAPI Python backend
│   ├── agents/               # AI pipeline agents
│   │   ├── architect.py      # Dataset profiling
│   │   ├── statistician.py   # Statistical analysis
│   │   ├── visualizer.py     # Chart generation
│   │   └── insights.py       # LLM insights
│   ├── core/                 # Shared state, constants, utils
│   ├── api.py                # API endpoints
│   ├── analysis_history.py   # History DB operations
│   └── requirements.txt      # Python dependencies
├── frontend/                 # React + Vite frontend
│   └── src/
│       └── datapulse_dashboard.jsx
├── start-backend.bat         # One-click backend start
├── start-frontend.bat        # One-click frontend start
└── .env                      # Secrets (NOT on GitHub — get from project owner)
```

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `Python not found` | Reinstall Python and check "Add Python to PATH" |
| `npm not found` | Install Node.js from nodejs.org |
| `.env file is missing` | Ask the project owner for the `.env` file |
| Backend crashes on start | Check the terminal output — usually a missing dependency |
| Charts not showing | Wait a few more seconds, the AI pipeline takes 5–15s |
| Port already in use | Close other terminals and try again |
