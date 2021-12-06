# deng_g7
Data Engineering 2021 Project Theme: Internet Memes

### Links
Project Tasks plan
https://docs.google.com/document/d/14JYqHhRvzPlYPleI7deWg6MC7dNq1EeLx6oO1mqdCVE

#### TODO
    
    arflow operators
        
        (Cleansing/transformation)
        take updated kym_unique_filter_1 -> create csv with Tags table for ingestion 
        take updated kym_unique_filter_1 -> create csv with Examples table for ingestion
        ..
        
        (Augmentation)
        from raw Google vision data -> create csv for ingestion
        dandelion similarity table(s)
        LDATopics table(s)
        LDATopicKeywords table(s)
        ..
        
        (Ingestion)
        Creation of postgres database according to star squema          
        ingestion of data/csv into postgres database according to star squema          
        Creation of neo4j database according to graph squema          
        ingestion of data/csv into neo4j database according to star squema          
        ..
        
        (Queries)
        ..
    
    data models
        design star squema
        design graph-based
    
    presentation slides
        (google slides)
        for early feedback
        final presentation
    
    report
        (overleaf)
        describes the design decision behind every pipeline step

    minor
        rename 'Notable Examples' in schema to 'Examples'
    
    
    Rodrigo
        .- update filter_1 operator to keep only memes category and status in ['confirmed', 'submission'] -> Rodrigo
        .- new operator using katrin's script putinTables -> should produce already the file with core data for ingestion
        
        .- check why neither ninja nor template_searchpath worked in the
           bash operator
        .- remove postgres sections from pipeline 1
        .- change some keys in the format "_id" -> "id" ?
        .- make sure we are dropping _search_keywords
        .- write a version of the 'combined_examples' using pattern matching from python 3.10
