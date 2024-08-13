FROM public.ecr.aws/unocha/python:3.12-stable

WORKDIR /srv

COPY . .

RUN pip --no-cache-dir install --upgrade -r requirements.txt

CMD ["python3", "run.py"]
