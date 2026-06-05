{% macro duckdb__get_catalog(information_schema, schemas) -%}
  {%- set query -%}
    with tables as (
      {{ duckdb__get_catalog_tables(information_schema, schemas) }}
    ),
    columns as (
      select *
    from (
        select
            c.database_name as table_database,
            c.schema_name as table_schema,
            c.table_name as table_name,
            c.column_name as column_name,
            cast(c.column_index as decimal) as column_index,
            c.data_type as column_type,
            c.comment as column_comment
        from duckdb_columns() c
        where upper(c.database_name) = upper('{{ information_schema.database }}')
    ) catalog_columns
      {%- if schemas -%}
        where (
            {%- for schema in schemas -%}
                (
                    upper(table_schema) = upper('{{ schema }}')
                ){%- if not loop.last %} or {% endif -%}
            {%- endfor -%}
        )
      {%- endif -%}
    )
    select
        tables.table_database,
        tables.table_schema,
        tables.table_name,
        tables.table_type,
        tables.table_comment,
        columns.column_name,
        columns.column_index,
        columns.column_type,
        columns.column_comment,
        cast(null as varchar) as table_owner
    from tables
    join columns using (table_database, table_schema, table_name)
    order by table_schema, table_name, column_index
  {%- endset -%}
  {{ return(run_query(query)) }}
{%- endmacro %}
