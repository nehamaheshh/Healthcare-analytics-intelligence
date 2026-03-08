# Push to GitHub

Your repo is committed locally. To put it on GitHub:

## 1. Create a new repository on GitHub

1. Go to [github.com/new](https://github.com/new).
2. **Repository name:** e.g. `solace-ops-intelligence` (or any name you like).
3. **Description (optional):** `Patient Advocacy Funnel & Bottleneck Detection - analytics pipeline and Streamlit dashboard`
4. Choose **Public**.
5. **Do not** add a README, .gitignore, or license (we already have them).
6. Click **Create repository**.

## 2. Push from your machine

In a terminal, from the project folder (`Solace_project`), run (replace `YOUR_USERNAME` and `REPO_NAME` with your GitHub username and repo name):

```bash
cd C:\Users\Admin\Desktop\Solace_project

git remote add origin https://github.com/YOUR_USERNAME/REPO_NAME.git
git branch -M main
git push -u origin main
```

**Example:** If your GitHub username is `jane` and the repo is `solace-ops-intelligence`:

```bash
git remote add origin https://github.com/jane/solace-ops-intelligence.git
git branch -M main
git push -u origin main
```

If GitHub asks for login, use your username and a **Personal Access Token** (not your password). Create one at: GitHub → Settings → Developer settings → Personal access tokens.

---

After pushing, anyone who clones the repo should run:

```bash
pip install -r requirements.txt
python scripts/generate_data.py
python scripts/load_duckdb.py
python scripts/run_pipeline.py
streamlit run app/streamlit_app.py
```
