FROM ubuntu:22.04

RUN apt-get update && apt-get install -y libreoffice

# run libreoffice in headless mode with a file from args
ENTRYPOINT ["libreoffice", "--headless", "--convert-to", "pdf"]
CMD ["/tmp/input.docx"]
