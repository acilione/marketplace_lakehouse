FROM maven:3.9.11-eclipse-temurin-21-alpine AS jars
WORKDIR /build
COPY docker/spark/pom.xml .
RUN mvn --batch-mode --no-transfer-progress dependency:copy-dependencies \
    -DincludeScope=runtime -DoutputDirectory=/jars

FROM apache/spark:4.1.3-python3

USER root
WORKDIR /opt/marketplace
COPY --from=jars /jars/ /opt/spark/jars/
COPY pyproject.toml README.md LICENSE ./
COPY requirements/ requirements/
COPY src/ src/
COPY apps/ apps/
COPY config/ config/
COPY contracts/ contracts/
COPY quality/ quality/
COPY docker/spark/metrics.properties /opt/spark/conf/metrics.properties
RUN python3 -m pip install --no-cache-dir --constraint requirements/runtime.lock . && \
    mkdir -p /var/lib/marketplace/metrics && chown -R 185:0 /var/lib/marketplace && \
    chown -R 185:0 /opt/marketplace && chmod -R g=u /opt/marketplace

ENV PYTHONPATH=/opt/marketplace/src \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
USER 185
