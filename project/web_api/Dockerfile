FROM python:3.12
WORKDIR /usr/src/app
COPY ./src ./
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
CMD ["uvicorn", "web:app", "--host", "0.0.0.0", "--port", "8000"]