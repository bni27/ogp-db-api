FROM python:3.11-slim
WORKDIR /code
COPY Pipfile Pipfile.lock /code/
RUN python -m pip install --upgrade pip
RUN pip install pipenv
RUN pipenv install --system --deploy
COPY ./app /code/app
CMD ["fastapi", "run", "app/main.py", "--port", "8080"]