FROM python:3.9-slim AS pythonapp

ARG PRIMARY_USER=rasgo
ARG HOME_DIR=/var/lib/rasgoql
ARG UID=1001
RUN adduser --system --uid ${UID} --home ${HOME_DIR} --shell /bin/false ${PRIMARY_USER}

ARG APP_HOME=${HOME_DIR}/app
RUN mkdir -p ${APP_HOME} && \
    mkdir -p ${HOME_DIR}/mount && \
    chown ${PRIMARY_USER} ${HOME_DIR}

# Dependencies for the Snowflake Python connector
RUN apt-get update --assume-yes
RUN apt-get install --assume-yes build-essential libssl-dev libffi-dev

# Configure the entrypoint script
RUN apt-get install --assume-yes gettext-base # provides envsubst used by next script
COPY python-entrypoint.sh /usr/bin/entrypoint
RUN chmod +x /usr/bin/entrypoint
ENTRYPOINT ["/usr/bin/entrypoint"]

USER ${PRIMARY_USER}
WORKDIR ${HOME_DIR}


FROM pythonapp AS rasgoql
ARG HOME_DIR=/var/lib/rasgoql
COPY rasgoql/requirements.txt ${HOME_DIR}

RUN cd ${HOME_DIR} && \
    pip install -r requirements.txt

# Used to deploy to PyPi
RUN pip install --upgrade twine

# Add the PyPi RC file template, not rendered until container start
# time because it contains secret vaules.
COPY pypirc.template ${HOME_DIR}/.pypirc.template

ARG PRIMARY_USER=rasgo
ARG HOME_DIR=/var/lib/rasgoql
USER ${PRIMARY_USER}
WORKDIR ${HOME_DIR}