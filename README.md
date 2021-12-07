# deng_g7
Data Engineering 2021 Project Theme: Internet Memes

### Links
Project Tasks plan
https://docs.google.com/document/d/14JYqHhRvzPlYPleI7deWg6MC7dNq1EeLx6oO1mqdCVE

#### TODO

    arflow operators

        (Cleansing/transformation)
        take updated kym_unique_filter_1 -> create csv with Tags table for ingestion -> Rodrigo
        take updated kym_unique_filter_1 -> create csv with Examples table for ingestion -> Rodrigo
        new operator using katrin's script putinTables -> should produce already the file with core data for ingestion -> Rodrigo
        ..

        (Augmentation)
        from raw Google vision data -> create csv for ingestion -> Liisi
        dandelion similarity table(s) --> Katrin
        LDATopics table(s) --> Katrin
        LDATopicKeywords table(s) --> Katrin
        ..

        (Ingestion)
        Creation of postgres database according to star schema -> Liisi
        ingestion of data/csv into postgres database according to star schema -> Liisi
        Creation of neo4j database according to graph schema
        ingestion of data/csv into neo4j database according to star schema
        ..

        (Queries)
        ..

    data models
        design star schema -> Liisi
        design graph-based -> Liisi could do, but not sure how exactly :)

    presentation slides
        (google slides)
        for early feedback
        final presentation

    report
        (overleaf)
        describes the design decision behind every pipeline step

    minor
        sign-up for early presentations
        rename 'Notable Examples' in schema to 'Examples'
        check why neither ninja nor template_searchpath worked in the bash operator
        remove postgres sections from pipeline 1
        write a version of the 'combined_examples' using pattern matching from python 3.10 -> R
