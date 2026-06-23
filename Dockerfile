# Dockerfile — API image (PLACEHOLDER, do not build yet)
# TODO: define the production image for the FastAPI app.
# Intended outline:
#   FROM python:3.12-slim
#   install uv -> copy pyproject -> uv pip sync -> copy app
#   CMD: gunicorn -k uvicorn.workers.UvicornWorker app.main:app
# See INSTALL.md and STOCK_API_MASTER_PLAN.md.
