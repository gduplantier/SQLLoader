# #!/usr/bin/bash

java -jar build/libs/SQLLoader*.jar \
    --spring.config.location=src/main/resources/application.properties \
    --jdbc.driver.password=oracle \
    --marklogic.password=admin \
    --marklogic.collections=oracle,test,scott,emp \
    --marklogic.uriid=EMPNO \
    --sqlloader.table=scott.emp \
    --sqlloader.metadata=source,oracle\;owner,marklogic\;DbSchema,scott\;table,emp
