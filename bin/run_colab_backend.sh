#! /bin/bash
poetry run jupyter notebook \
    --NotebookApp.allow_origin='https://colab.research.google.com' \
    --port=8888 \
    --no-browser \
    --NotebookApp.port_retries=0


