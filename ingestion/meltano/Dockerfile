FROM meltano/meltano:v4.2.2-python3.11

WORKDIR /project

COPY meltano.yml ./meltano.yml

RUN meltano install

ENTRYPOINT ["meltano"]

CMD ["run", "banvic_full_ingestion"]
