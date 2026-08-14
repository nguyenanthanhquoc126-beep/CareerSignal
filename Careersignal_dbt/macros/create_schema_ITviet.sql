{% macro create_schema_ITviet()  %}
    {%set sql%}
        Create Schema IF NOT EXISTS nessie.silver
        WITH(
            LOCATION = 's3a://warehouse/silver/'
        )

    {% endset %}
    {% do run_query(sql)%}
{% endmacro %}