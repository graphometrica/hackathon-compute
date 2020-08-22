# Machine Learning


## Run explanation service

```
pip install -r requirements.txt
python main.py
gunicorn --threads NUM_THREADS services.explanation_service:App
```

