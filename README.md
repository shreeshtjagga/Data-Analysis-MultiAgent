# Data Analysis Multi-Agent System

## Team Responsibilities 
If anyone is unable to handle the  stuff chill , share the work 

### Shreesht

* backend/core/graph.py
* backend/core/state.py
* backend/core/utils.py
* backend/agents/architect.py
* requirements.txt
* docker-compose.yml
* .gitignore

---

### Vaibhav

* backend/agents/statistician.py
* backend/agents/insights.py
* backend/models/schemas.py

---

### Naga Balaji

* backend/api.py
* backend/app.py
* .env.example
* .github/workflows/python-checks.yml

---

### Saketh Ram

* backend/agents/visualizer.py
* data/sample_data.csv
* data/test_data/good.csv
* data/test_data/bad.csv
* data/test_data/messy.csv

---

## Project Structure

```
Data-Analysis-MultiAgent/
│
├── backend/
│   ├── agents/
│   │   ├── architect.py
│   │   ├── statistician.py
│   │   ├── visualizer.py
│   │   └── insights.py
│   │
│   ├── core/
│   │   ├── state.py
│   │   ├── graph.py
│   │   └── utils.py
│   │
│   ├── models/
│   │   └── schemas.py
│   │
│   ├── app.py
│   └── api.py
│
├── data/
│   ├── sample_data.csv
│   └── test_data/
│       ├── good.csv
│       ├── bad.csv
│       └── messy.csv
│
├── .github/workflows/
│   └── python-checks.yml
│
├── requirements.txt
├── .env.example
├── .gitignore
├── docker-compose.yml
└── README.md
```

---

## Workflow

```
git pull origin main
git checkout yourname
git add .
git commit -m "message"
git push origin yourname
```

---

## Rules

* Work only in your assigned files
* Do not push directly to main
* Test code before pushing
* Use clear commit messages
* Review before you merge
* If code fails don't panic and raise issue or revert back to commit
* Be optimal with the code part


---

## System Flow

Architect → Statistician → Visualizer → Insights
