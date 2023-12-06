SCHEMA=SCOTT
SOURCE=Oracle
while read table; do
    echo $line
    java -jar build/libs/SQLLoader*.jar \
        --spring.config.location=src/main/resources/application.properties \
        --jdbc.driver.password=oracle \
        --marklogic.password=admin \
        --marklogic.collections=$SOURCE,$SCHEMA,$table \
        --sqlloader.source=$SOURCE \
        --sqlloader.schema=$SCHEMA \
        --sqlloader.table=$table \
        --sqlloader.metadata=source,$SOURCE\;owner,marklogic\;DbSchema,$SCHEMA\;table,$table
done <./tables.txt
