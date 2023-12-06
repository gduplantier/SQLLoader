SCHEMA=SCOTT
SOURCE=Oracle
while read table; do
    echo $line
    java -jar build/libs/SQLLoader*.jar \
        --spring.config.location=src/main/resources/application.properties \
        --jdbc.driver.password=oracle \
        --marklogic.password=admin \
        --marklogic.collections=$SOURCE,$SCHEMA,$table \
        --sqlloader.table=$table \
        --sqlloader.schema=$SCHEMA \
        --sqlloader.metadata=source,$SOURCE\;owner,marklogic\;DbSchema,$SCHEMA\;table,$table
done <./tables.txt
