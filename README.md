# Data Analysis Agent

## Team Workflow Guidelines

### Branch Strategy

* `main` - Stable and final code
* Each member must use their own branch:

  * `Shreesht`
  * `Naga Balaji`
  * `Saketh Ram`
  * `Vaibhav`

---

## Workflow

1. Pull latest changes

```
git pull origin main
```

2. Switch to your branch

```
git checkout yourname
```

3. Add and commit changes

```
git add .
git commit -m "clear message"
```

4. Push your branch

```
git push origin yourname
```

5. Create Pull Request on GitHub

* Base: `main`
* Compare: `yourname`
* Merge after review

---

## Rules

### Do

* Work only in your own branch
* Pull latest code before starting
* Write meaningful commit messages
* Keep commits small and focused
* Create Pull Requests for merging

### Do Not (Strictly things to follow)

* Do not push directly to `main`
* Do not rename or delete branches without team discussion
* Do not commit directly to another member’s branch
* Do not leave unresolved merge conflicts
* Do not commit broken or non-working code to your branch
* Do not push large unnecessary files (videos, datasets, node_modules, etc.)
* Do not hardcode sensitive data (API keys, passwords, tokens)
* Do not change project structure without informing the team
* Do not modify shared configuration files without discussion
* Do not ignore code review comments
* Do not create unclear or vague commit messages (e.g., "update", "fix")
* Do not commit commented-out or unused code
* Do not duplicate code unnecessarily
* Do not skip testing before pushing changes
* Do not push incomplete features without marking them clearly
* Do not overwrite others’ work while resolving conflicts
* Do not merge your own Pull Request without review
* Do not create multiple branches for the same task unnecessarily
* Do not delay Pull Requests for too long after completing work
* Do not break existing functionality while adding new features
* Do not ignore coding standards followed by the team

---

## Daily Flow

```
git pull origin main
git checkout yourname
git add .
git commit -m "message"
git push
```

---

## Summary

All development must happen in individual branches. Changes should be merged into `main` only through Pull Requests after review only.
Balaji follow these ,don't tell "Naku telvadhi" 
