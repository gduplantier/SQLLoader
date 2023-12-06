# #!/usr/bin/bash

TABLE=DEPT
SCHEMA=SCOTT
SOURCE=Oracle

java -jar build/libs/SQLLoader*.jar \
    --spring.config.location=src/main/resources/application.properties \
    --jdbc.driver.password=oracle \
    --marklogic.password=admin \
    --marklogic.collections=$SOURCE,$SCHEMA,$TABLE \
    --sqlloader.source=$SOURCE \
    --sqlloader.schema=$SCHEMA \
    --sqlloader.table=$TABLE \
    --sqlloader.metadata=source,$SOURCE\;owner,marklogic\;DbSchema,$SCHEMA\;table,$TABLE
