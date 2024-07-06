FROM python:3.11-slim
WORKDIR /app
COPY Pipfile Pipfile.lock /app/
RUN python -m pip install --upgrade pip
RUN pip install pipenv
RUN pipenv install --system --deploy
COPY run.sh /app/
COPY main.py /app/
EXPOSE ${PORT}
CMD ["./run.sh"]
