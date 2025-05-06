FROM python:3.12-slim
ENV PYTHONIOENCODING=utf-8

ENV PYTHONPATH="/code:${PYTHONPATH}"

ARG COMPONENT_DIR="ex-kafka"

RUN pip install flake8


COPY /components/common/requirements-common.txt /code/
RUN pip install -r /code/requirements-common.txt

COPY /components/common /code/common/

COPY /components/${COMPONENT_DIR}/src /code/src/
COPY /components/${COMPONENT_DIR}/tests /code/tests/
COPY /components/${COMPONENT_DIR}/scripts /code/scripts/
COPY /components/${COMPONENT_DIR}/flake8.cfg /code/flake8.cfg

WORKDIR /code/

CMD ["python", "-u", "/code/src/component.py"]