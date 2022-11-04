-- {{ target.database}}


{{ 
    codegen.generate_source(
        schema_name='jaffle_shop',
        database_name='dbt-tutorial',
        generate_columns=True
    ) 
}}

{{ 
    codegen.generate_source(
        schema_name='stripe',
        database_name='dbt-tutorial',
        generate_columns=True
    ) 
}}