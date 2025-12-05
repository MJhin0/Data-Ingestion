.\venv\Scripts\Activate.ps1

python -m ingestion.src.main
pytest -v

git add .
git commit -m ""
git push


make a readme.md
user input for csv name


https://www.kaggle.com/datasets/bhanupratapbiswas/uber-data-analysis