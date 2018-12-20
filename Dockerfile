FROM unocha/hdx-scraper-baseimage:stable

WORKDIR /srv

COPY . .

CMD ["python3", "run.py"]
