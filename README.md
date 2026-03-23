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

### Do Not

* Do not push directly to `main`
* Do not use force push
* Do not work on another member’s branch
* Do not skip pulling before working
* Do not commit unnecessary files

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

All development must happen in individual branches. Changes should be merged into `main` only through Pull Requests after review.
